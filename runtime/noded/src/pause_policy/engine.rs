//! The policy's state and its synchronous steps: the paused table re-derived
//! from markers, the reclaim tick (`node_runtime._reclaim_paused_tick`), the
//! paused reclaim (`direct_service.reclaim_paused`, `_reclaim_paused`), the
//! escalation decision (`_escalate_paused`), the paused sampler pass and the
//! idle pause (`_idle_parking_loop`, `_idle_pause`). `mod.rs` drives them.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use serde_json::{Map, Value};

use super::budget::ReclaimBudget;
use super::decision::{Decision, MemoryDemand, decide_resident_wait, swap_room_bytes};
use super::fence::PauseFence;
use super::files::{DemandSource, Status, StatusFile};
use super::plan::{ESCALATION_CONCURRENCY, Key, PausedWait, RECLAIM_WINDOW_BYTES, note_reclaim, reclaim_stalled, relief_plan};
use super::pressure::PressureSource;
use super::resident::{ReclaimRequest, ReclaimResult, Reclaimer, ResidentSampler, bundle_cgroup_path, kernel_write};
use super::{Answered, Candidate, Clock, Escalated, Escalator, Inventory, PauseMechanism, PolicyConfig};
use crate::pause::Counter;
use crate::warden::Sandbox;

/// A forecast-only reclaim (demand alone, no measured pressure) waits this
/// long for a burst to settle into safe waits, then releases one at a time.
pub const FORECAST_GRACE_SECONDS: f64 = 1.0;
/// Paused samples are refreshed this often.
pub const SAMPLE_INTERVAL_SECONDS: f64 = 1.0;
/// Errors of one kind are logged at most this often.
const LOG_INTERVAL_SECONDS: f64 = 10.0;

/// A paused reclaim in place of the kernel's (a test seam).
pub type PausedReclaim = Arc<dyn Fn(&Key, u64) -> Result<ReclaimResult, String> + Send + Sync>;

/// Work the tick hands to the driver.
#[derive(Debug)]
pub enum Job {
    /// Reclaim this many bytes of a paused wait (a blocking worker).
    Reclaim(Key, u64),
    /// Ask the agent to hibernate this paused wait.
    Escalate(Key, Sandbox),
}

/// What one idle pause attempt did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdleOutcome {
    Paused,
    /// A marker exists: the reclaim tick adopts it.
    AlreadyPaused,
    /// The journal is not RUNNING/LIVE or its sentry is not alive.
    NotRunning,
    /// T or A is held: a transition or activity; the next tick decides.
    Busy,
    /// Activity since the observation (A's mtime moved).
    Active,
    /// The registration changed generation, ownership or kind.
    Gone,
    /// The recheck under the warden flock declined, or the pause failed.
    Declined,
}

#[derive(Default)]
struct Table {
    /// The paused waits, each with its adoption order (Python's dict order).
    waits: HashMap<Key, (u64, PausedWait)>,
    next_order: u64,
    forecast_since: Option<f64>,
    paused_sandboxes: u64,
    decision: Option<Decision>,
    next_sample: f64,
}

impl Table {
    fn insert(&mut self, key: Key, wait: PausedWait) {
        let order = self.waits.get(&key).map(|(order, _)| *order).unwrap_or_else(|| {
            self.next_order += 1;
            self.next_order
        });
        self.waits.insert(key, (order, wait));
    }

    /// Keys in adoption order.
    fn ordered(&self) -> Vec<Key> {
        let mut keys: Vec<(&u64, &Key)> = self.waits.iter().map(|(key, (order, _))| (order, key)).collect();
        keys.sort();
        keys.into_iter().map(|(_, key)| key.clone()).collect()
    }
}

#[derive(Default)]
struct IdleState {
    revision: Option<i64>,
    candidates: Vec<Candidate>,
    /// When this daemon first saw each candidate (monotonic): the first
    /// observation counts as activity, as Python's `setdefault(now)`.
    first_seen: HashMap<Key, f64>,
}

/// Logs one kind of error at most every LOG_INTERVAL_SECONDS.
struct RateLimited {
    last: Mutex<HashMap<&'static str, f64>>,
}

pub(crate) struct Deps {
    pub mechanism: Arc<dyn PauseMechanism>,
    pub fence: PauseFence,
    pub inventory: Arc<dyn Inventory>,
    pub escalator: Arc<dyn Escalator>,
    pub clock: Arc<dyn Clock>,
    pub pressure: Arc<dyn PressureSource>,
    pub demand: Arc<dyn DemandSource>,
    pub budget: ReclaimBudget,
    pub paused_reclaim: Option<PausedReclaim>,
}

pub struct Engine {
    pub(crate) config: PolicyConfig,
    pub(crate) mechanism: Arc<dyn PauseMechanism>,
    fence: PauseFence,
    inventory: Arc<dyn Inventory>,
    escalator: Arc<dyn Escalator>,
    pub(crate) clock: Arc<dyn Clock>,
    pressure: Arc<dyn PressureSource>,
    demand: Arc<dyn DemandSource>,
    pub(crate) budget: ReclaimBudget,
    paused_reclaim: Option<PausedReclaim>,
    pub(crate) sampler: ResidentSampler,
    /// `linux.cgroupsPath` of each sampled incarnation's bundle.
    cgroup_paths: Mutex<HashMap<Key, Option<String>>>,
    table: Mutex<Table>,
    idle: Mutex<IdleState>,
    status_file: Mutex<StatusFile>,
    pub(crate) answered: RwLock<Option<Answered>>,
    pub(crate) stopped: AtomicBool,
    logs: RateLimited,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn key_of(sandbox: &Sandbox) -> Key {
    (sandbox.sandbox_id.clone(), sandbox.generation)
}

fn is_running(record: &Map<String, Value>) -> bool {
    record.get("state").and_then(Value::as_str) == Some("running")
}

/// The journal says RUNNING with live authority and its sentry is the
/// process it names (pid and start ticks), as an exec start checks.
pub fn running_and_alive(record: &Map<String, Value>, proc_root: &std::path::Path) -> bool {
    if !is_running(record) || record.get("authority").and_then(Value::as_str) != Some("live") {
        return false;
    }
    let pid = record.get("sentry_pid").and_then(Value::as_u64).and_then(|pid| u32::try_from(pid).ok());
    let ticks = record.get("sentry_start_time_ticks").and_then(Value::as_u64);
    let (Some(pid), Some(ticks)) = (pid, ticks) else { return false };
    crate::runsc::start_time_ticks(proc_root, pid).is_ok_and(|now| now == ticks)
}

impl Engine {
    pub(crate) fn new(config: PolicyConfig, deps: Deps) -> Engine {
        let sampler = ResidentSampler::new(config.cgroup_root.clone(), config.proc_root.clone(), deps.clock.clone());
        let status_file = StatusFile::open(&config.state_root);
        Engine {
            mechanism: deps.mechanism,
            fence: deps.fence,
            inventory: deps.inventory,
            escalator: deps.escalator,
            clock: deps.clock,
            pressure: deps.pressure,
            demand: deps.demand,
            budget: deps.budget,
            paused_reclaim: deps.paused_reclaim,
            sampler,
            cgroup_paths: Mutex::new(HashMap::new()),
            table: Mutex::new(Table::default()),
            idle: Mutex::new(IdleState::default()),
            status_file: Mutex::new(status_file),
            answered: RwLock::new(None),
            stopped: AtomicBool::new(false),
            logs: RateLimited { last: Mutex::new(HashMap::new()) },
            config,
        }
    }

    pub(crate) fn stopping(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    fn log(&self, kind: &'static str, message: impl std::fmt::Display) {
        let now = self.clock.monotonic();
        let mut last = lock(&self.logs.last);
        if last.get(kind).is_none_or(|at| now - at >= LOG_INTERVAL_SECONDS) {
            last.insert(kind, now);
            eprintln!("ucloud-noded: pause policy: {kind}: {message}");
        }
    }

    // The paused table.

    /// One paused wait's metadata.
    pub fn paused_wait(&self, key: &Key) -> Option<PausedWait> {
        lock(&self.table).waits.get(key).map(|(_, wait)| wait.clone())
    }

    /// Every paused wait, in adoption order.
    pub fn paused_waits(&self) -> Vec<(Key, PausedWait)> {
        let table = lock(&self.table);
        table.ordered().into_iter().map(|key| {
            let wait = table.waits[&key].1.clone();
            (key, wait)
        }).collect()
    }

    /// A pause just landed (Python's park path): the wait starts now, with
    /// the relay's predicted remaining wait when it gave one.
    pub fn record_pause(&self, key: Key, expected_wait_seconds: Option<f64>, local_request_id: &str) {
        let now = self.clock.monotonic();
        let wait = PausedWait {
            expected_until: expected_wait_seconds.map(|seconds| now + seconds),
            local_request_id: local_request_id.to_string(),
            ..PausedWait::new(now)
        };
        lock(&self.table).insert(key, wait);
    }

    #[cfg(test)]
    pub(crate) fn with_wait<T>(&self, key: &Key, change: impl FnOnce(&mut PausedWait) -> T) -> Option<T> {
        lock(&self.table).waits.get_mut(key).map(|(_, wait)| change(wait))
    }

    #[cfg(test)]
    pub(crate) fn rewind_forecast(&self, seconds: f64) {
        if let Some(since) = lock(&self.table).forecast_since.as_mut() {
            *since -= seconds;
        }
    }

    /// Markers are the truth: drop entries whose marker went (thawed by an
    /// activity path), adopt markers without one (restarts, the agent's relay
    /// parks, commit exports). Returns the markers.
    fn rederive(&self) -> std::io::Result<HashSet<Key>> {
        let markers: HashSet<Key> = self.mechanism.paused_keys()?.into_iter().collect();
        let now = self.clock.monotonic();
        let mut table = lock(&self.table);
        table.waits.retain(|key, _| markers.contains(key));
        let mut adopted: Vec<&Key> = markers.iter().filter(|key| !table.waits.contains_key(*key)).collect();
        adopted.sort();
        for key in adopted {
            table.insert(key.clone(), PausedWait::new(now));
        }
        table.paused_sandboxes = markers.len() as u64;
        Ok(markers)
    }

    /// `_reclaim_paused_tick`: relieve measured pressure by swapping paused
    /// sandboxes out, else by hibernating them. Returns the work to start.
    pub fn tick(&self) -> Vec<Job> {
        if let Err(error) = self.rederive() {
            self.log("markers", error);
            return Vec::new();
        }
        // Closed admission (drain) bills nothing (the demand source).
        let demand = self.demand.demand();
        let pressure = self.pressure.sample();
        let decision = decide_resident_wait(&pressure, demand, false, false);
        // Growth forecasts are guarantees, not allocated pages: a burst gets a
        // second to settle into safe waits, then waits are released one at a
        // time; only measured pressure reclaims in parallel.
        let forecast_only = decision.reclaim() && !decide_resident_wait(&pressure, MemoryDemand::default(), false, false).reclaim();
        let now = self.clock.monotonic();
        let mut table = lock(&self.table);
        table.decision = Some(decision);
        if !forecast_only {
            table.forecast_since = None;
        } else if table.forecast_since.is_none() {
            table.forecast_since = Some(now);
        }
        let settling = forecast_only && table.forecast_since.is_some_and(|since| now - since < FORECAST_GRACE_SECONDS);
        if !decision.reclaim() || table.waits.is_empty() || settling || self.stopping() {
            return Vec::new();
        }
        for (key, (_, wait)) in table.waits.iter_mut() {
            let sample = self.sampler.get(key); // Refreshed each second by the sampler pass.
            wait.resident_bytes = sample.as_ref().map(|sample| sample.current_bytes);
            wait.swapped_bytes = sample.map_or(0, |sample| sample.swap_bytes);
        }
        let order = table.ordered();
        let (reclaims, escalations) = {
            let view: Vec<(&Key, &PausedWait)> = order.iter().map(|key| (key, &table.waits[key].1)).collect();
            relief_plan(&decision, &view, self.clock.monotonic(), swap_room_bytes(&pressure))
        };
        let reclaiming = table.waits.values().filter(|(_, wait)| wait.reclaiming != 0).count();
        let escalating = table.waits.values().filter(|(_, wait)| wait.escalating).count();
        let concurrency = if forecast_only { 1 } else { self.budget.concurrency };
        let mut jobs = Vec::new();
        for (key, target) in reclaims.into_iter().take(concurrency.saturating_sub(reclaiming)) {
            if let Some((_, wait)) = table.waits.get_mut(&key) {
                wait.reclaiming = target;
                jobs.push(Job::Reclaim(key, target));
            }
        }
        for key in escalations.into_iter().take(ESCALATION_CONCURRENCY.saturating_sub(escalating)) {
            // A marker without its owned incarnation has nothing to hibernate.
            let sandbox = match self.inventory.owned_one(&key.0) {
                Ok(Some(candidate)) if candidate.sandbox.generation == key.1 => candidate.sandbox,
                Ok(_) => continue,
                Err(error) => {
                    self.log("registry", error);
                    continue;
                }
            };
            if let Some((_, wait)) = table.waits.get_mut(&key) {
                wait.escalating = true;
                jobs.push(Job::Escalate(key, sandbox));
            }
        }
        jobs
    }

    // Sampling.

    /// One incarnation's fresh sample from its journal's sentry and its
    /// bundle's cgroup path (`_sample_resident`).
    fn sample_one(&self, key: &Key, sandbox: &Sandbox) -> Option<super::resident::ResidentSample> {
        let record = self.mechanism.journal(&key.0, key.1).ok()??;
        let pid = record.get("sentry_pid").and_then(Value::as_u64).filter(|pid| *pid != 0)?;
        let ticks = record.get("sentry_start_time_ticks").and_then(Value::as_u64).filter(|ticks| *ticks != 0)?;
        let cached = lock(&self.cgroup_paths).get(key).cloned();
        let path = match cached {
            Some(path) => path,
            None => {
                let path = bundle_cgroup_path(&sandbox.bundle).ok()?;
                lock(&self.cgroup_paths).insert(key.clone(), path.clone());
                path
            }
        };
        self.sampler.sample(key, u32::try_from(pid).ok()?, ticks, &sandbox.container_id, path.as_deref())
    }

    /// The paused half of `refresh_resident_memory`: sample every owned,
    /// parkable incarnation with a marker; forget every other.
    pub fn sample_paused(&self) {
        let markers: HashSet<Key> = match self.mechanism.paused_keys() {
            Ok(keys) => keys.into_iter().collect(),
            Err(error) => return self.log("markers", error),
        };
        let owned = match self.inventory.owned() {
            Ok(owned) => owned,
            Err(error) => return self.log("registry", error),
        };
        let candidates: Vec<Candidate> = owned.into_iter().filter(|candidate| candidate.parkable && markers.contains(&key_of(&candidate.sandbox))).collect();
        let keys: HashSet<Key> = candidates.iter().map(|candidate| key_of(&candidate.sandbox)).collect();
        self.sampler.retain(&keys);
        lock(&self.cgroup_paths).retain(|key, _| keys.contains(key));
        for candidate in candidates {
            self.sample_one(&key_of(&candidate.sandbox), &candidate.sandbox);
        }
    }

    /// Sample if a second has passed since the last pass.
    pub fn sample_if_due(&self) {
        let now = self.clock.monotonic();
        {
            let mut table = lock(&self.table);
            if now < table.next_sample {
                return;
            }
            table.next_sample = now + SAMPLE_INTERVAL_SECONDS;
        }
        self.sample_paused();
    }

    // Paused reclaim.

    /// `reclaim_paused`: swap out a paused runtime's memory without any
    /// lifecycle lock. A thaw anywhere supersedes it from before its first
    /// prefetch read (the marker flock), so the next window does not start.
    fn kernel_reclaim(&self, key: &Key, target: u64) -> Result<ReclaimResult, String> {
        let (id, generation) = (key.0.as_str(), key.1);
        let candidate = self.inventory.owned_one(id)?.filter(|candidate| candidate.sandbox.generation == generation);
        let Some(candidate) = candidate else { return Err("paused reclaim no longer owns this generation".into()) };
        let paused = || self.mechanism.settled(id, generation);
        if !paused() {
            return Ok(ReclaimResult { requested_bytes: 0, reclaimed_bytes: 0, elapsed_seconds: 0.0, refault_file_pages: 0, reason: "superseded" });
        }
        let captured = self.mechanism.journal(id, generation)?;
        let sample = self.sample_one(key, &candidate.sandbox);
        let (Some(captured), Some(sample)) = (captured, sample) else {
            return Err("paused reclaim requires a measured paused incarnation".into());
        };
        if !is_running(&captured) {
            return Err("paused reclaim requires a measured paused incarnation".into());
        }
        let is_current = || !self.stopping() && paused() && self.mechanism.journal(id, generation).ok().flatten().as_ref() == Some(&captured);
        let request = ReclaimRequest { target_bytes: target, window_bytes: RECLAIM_WINDOW_BYTES, swappiness: 200 };
        Ok(Reclaimer { sampler: &self.sampler, write: kernel_write() }.reclaim(key, &sample, request, &is_current, Some(&self.budget)))
    }

    /// `_reclaim_paused`: one reclaim job, then its accounting. Any error
    /// still returns the slot and counts a stall.
    pub fn run_reclaim(&self, key: &Key, target: u64) {
        let result = match &self.paused_reclaim {
            Some(reclaim) => reclaim(key, target),
            None => self.kernel_reclaim(key, target),
        };
        if let Err(error) = &result {
            self.log("reclaim", error);
        }
        let result = result.ok();
        let stalled = reclaim_stalled(result.as_ref(), target);
        {
            let now = self.clock.monotonic();
            let mut table = lock(&self.table);
            if let Some((_, wait)) = table.waits.get_mut(key) {
                wait.reclaiming = 0;
                note_reclaim(wait, stalled, now);
            }
        }
        let stats = self.mechanism.stats();
        let mut counts = vec![(Counter::PauseReclaimStalls, f64::from(u8::from(stalled)))];
        if let Some(result) = &result {
            counts.extend([
                (Counter::PauseReclaims, 1.0),
                (Counter::PauseReclaimedBytes, result.reclaimed_bytes as f64),
                (Counter::PauseReclaimMsTotal, result.elapsed_seconds * 1000.0),
                (Counter::PauseReclaimCancellations, f64::from(u8::from(result.reason == "superseded"))),
            ]);
        }
        stats.add(&counts);
        stats.stopped(result.as_ref().map_or("raised", |result| result.reason));
    }

    // Escalation.

    /// `_escalate_paused`: ask the agent to hibernate one paused wait through
    /// its durable park. Skipped when the wait's call was answered (a local
    /// wait thaws it) or its marker is gone (activity won). Any error returns
    /// the slot; the next tick decides again.
    pub async fn run_escalation(&self, key: Key, sandbox: Sandbox) {
        let answered = self.answered.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        let skip = answered.is_some_and(|answered| answered(&key.0, key.1)) || !self.mechanism.is_paused(&key.0, key.1);
        let outcome = if skip { None } else { Some(self.escalator.escalate(&sandbox).await) };
        if let Some((_, wait)) = lock(&self.table).waits.get_mut(&key) {
            wait.escalating = false;
        }
        match outcome {
            Some(Ok(Escalated::Parked)) => self.mechanism.stats().add(&[(Counter::PauseEscalations, 1.0)]),
            Some(Err(error)) => self.log("escalation", format!("{}: {error}", key.0)),
            _ => {}
        }
    }

    // Status.

    /// What the next status write reports.
    pub fn status(&self) -> Status {
        let table = lock(&self.table);
        let decision = table.decision;
        Status {
            pause_stats: self.mechanism.stats().snapshot(),
            paused_sandboxes: table.paused_sandboxes,
            reason: decision.map_or("resident_headroom", |decision| decision.reason),
            reclaim_target_bytes: decision.map_or(0, |decision| decision.target_bytes),
        }
    }

    pub fn write_status(&self) -> Option<Value> {
        let status = self.status();
        match lock(&self.status_file).write(&status) {
            Ok(document) => Some(document),
            Err(error) => {
                self.log("status", error);
                None
            }
        }
    }

    // The idle pause.

    /// Seconds without activity: the smaller of this daemon's monotonic
    /// observation (the first sight counts as activity) and the time since
    /// A's mtime, the activity clock execs and the agent's marks touch.
    fn idle_for(&self, key: &Key, monotonic: f64, wall: f64) -> f64 {
        let first = *lock(&self.idle).first_seen.entry(key.clone()).or_insert(monotonic);
        let mut idle = (monotonic - first).max(0.0);
        if let Some(touched) = self.fence.activity_mtime(&key.0) {
            idle = idle.min((wall - touched).max(0.0));
        }
        idle
    }

    /// The candidates: owned, parkable, not managed (managed agents own their
    /// park points through the model-wait protocol). Re-read only when the
    /// registry's activity revision moved.
    fn idle_candidates(&self) -> Vec<Candidate> {
        let revision = match self.inventory.revision() {
            Ok(revision) => revision,
            Err(error) => {
                self.log("registry", error);
                return lock(&self.idle).candidates.clone();
            }
        };
        if lock(&self.idle).revision == Some(revision) {
            return lock(&self.idle).candidates.clone();
        }
        let owned = match self.inventory.owned() {
            Ok(owned) => owned,
            Err(error) => {
                self.log("registry", error);
                return lock(&self.idle).candidates.clone();
            }
        };
        let candidates: Vec<Candidate> = owned.into_iter().filter(|candidate| candidate.parkable && !candidate.managed).collect();
        let now = self.clock.monotonic();
        let mut idle = lock(&self.idle);
        let keys: HashSet<Key> = candidates.iter().map(|candidate| key_of(&candidate.sandbox)).collect();
        idle.first_seen.retain(|key, _| keys.contains(key));
        for key in keys {
            idle.first_seen.entry(key).or_insert(now);
        }
        idle.revision = Some(revision);
        idle.candidates = candidates.clone();
        candidates
    }

    /// One pass of the idle loop: pause each candidate idle for at least
    /// `idle_park_seconds`.
    pub async fn idle_pass(&self) -> Vec<(Key, IdleOutcome)> {
        let threshold = self.config.idle_park_seconds;
        let mut outcomes = Vec::new();
        if threshold <= 0.0 {
            return outcomes;
        }
        let (monotonic, wall) = (self.clock.monotonic(), self.clock.wall());
        for candidate in self.idle_candidates() {
            if self.stopping() {
                break;
            }
            let key = key_of(&candidate.sandbox);
            if self.idle_for(&key, monotonic, wall) < threshold {
                continue;
            }
            let outcome = self.idle_pause(&candidate).await;
            outcomes.push((key, outcome));
        }
        outcomes
    }

    /// `_idle_pause`, as a daemon transition (spec §6.2 item 1): T and A
    /// exclusive and non-blocking, the registration and idleness rechecked
    /// under them, then the journal and sentry rechecked under the warden
    /// flock, then the pause.
    pub async fn idle_pause(&self, candidate: &Candidate) -> IdleOutcome {
        let sandbox = &candidate.sandbox;
        let key = key_of(sandbox);
        if self.mechanism.is_paused(&key.0, key.1) {
            return IdleOutcome::AlreadyPaused; // The reclaim tick adopts every marker.
        }
        let proc_root = self.config.proc_root.clone();
        // Cheap before any lock: a parked, dead or reconciling runtime waits
        // for the agent (it reconciles on the next activity).
        match self.mechanism.journal(&key.0, key.1) {
            Ok(Some(record)) if running_and_alive(&record, &proc_root) => {}
            _ => return IdleOutcome::NotRunning,
        }
        let hold = match self.fence.try_exclusive(&key.0) {
            Ok(Some(hold)) => hold,
            Ok(None) => return IdleOutcome::Busy,
            Err(error) => {
                self.log("fence", error);
                return IdleOutcome::Busy;
            }
        };
        match self.inventory.owned_one(&key.0) {
            Ok(Some(current)) if current.sandbox.generation == key.1 && current.parkable && !current.managed => {}
            Ok(_) => return IdleOutcome::Gone,
            Err(error) => {
                self.log("registry", error);
                return IdleOutcome::Gone;
            }
        }
        // With A held exclusively no activity can start; one may have ended
        // since the observation.
        if self.idle_for(&key, self.clock.monotonic(), self.clock.wall()) < self.config.idle_park_seconds {
            return IdleOutcome::Active;
        }
        let check = Box::new(move |record: &Map<String, Value>| running_and_alive(record, &proc_root));
        let outcome = match self.mechanism.pause_checked(sandbox.clone(), check).await {
            Ok(true) => {
                let now = self.clock.monotonic();
                let mut table = lock(&self.table);
                if !table.waits.contains_key(&key) {
                    table.insert(key, PausedWait::new(now));
                }
                IdleOutcome::Paused
            }
            Ok(false) => IdleOutcome::Declined,
            Err(error) => {
                self.log("pause", format!("{}: {error}", key.0));
                IdleOutcome::Declined
            }
        };
        hold.release();
        outcome
    }
}
