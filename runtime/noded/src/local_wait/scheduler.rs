//! The flow table and the 10 ms policy (`local_wait.py`
//! `LocalWaitScheduler.observe`, `answered`, `_submit`, `_run`, `tick`).
//!
//! Pauses and thaws never run on the packet or policy thread: `submit` hands
//! them to the executor, at most one per sandbox at a time. They fail fast on
//! any activity ("busy"), and the next tick decides again.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use serde_json::{Map, Value, json};
use tokio::sync::Semaphore;

use super::flow::{ANSWERED_WATCH_SECONDS, Flow, cpu_usage_usec};
use super::packet::RelayPacket;
use crate::warden::Sandbox;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
/// Runs an action's future to completion (the tokio runtime in production).
pub(crate) type Spawn = Arc<dyn Fn(BoxFuture<()>) + Send + Sync>;
pub(crate) type Clock = Arc<dyn Fn() -> f64 + Send + Sync>;
/// Whether the call that made the policy pause is still unanswered.
pub(crate) type Recheck = Box<dyn Fn() -> bool + Send + Sync>;

/// Errors of one sandbox are logged at most this often.
const LOG_EVERY_SECONDS: f64 = 10.0;

/// A sandbox the scheduler may pause: its runtime identity, guest address
/// and cgroup `cpu.stat` (Python `WaitCandidate`).
#[derive(Clone, Debug)]
pub struct WaitCandidate {
    pub sandbox: Sandbox,
    pub guest: Ipv4Addr,
    pub cpu_stat: PathBuf,
}

impl WaitCandidate {
    pub fn key(&self) -> (String, u64) {
        (self.sandbox.sandbox_id.clone(), self.sandbox.generation)
    }
}

/// The node's waitable sandboxes: owned, parkable, managed-process
/// sandboxes with a network lease.
pub trait CandidateSource: Send + Sync {
    fn candidates(&self) -> Result<Vec<WaitCandidate>, String>;
    /// Whether this incarnation is still the owned registration (rechecked
    /// under the fence before a pause).
    fn current(&self, sandbox_id: &str, generation: u64) -> bool;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrowthAction {
    /// The guest is blocked on its model call (Python `observe_managed_wait`).
    Wait,
    /// Its answer arrived (Python `resume_managed_continuation`).
    Activate,
}

impl GrowthAction {
    pub fn as_str(self) -> &'static str {
        match self {
            GrowthAction::Wait => "wait",
            GrowthAction::Activate => "activate",
        }
    }
}

/// One item of `POST /internal/v1/growth/events`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrowthEvent {
    pub action: GrowthAction,
    pub sandbox_id: String,
    pub generation: u64,
    pub request_id: String,
}

impl GrowthEvent {
    /// `{"action", "sandbox_id", "generation", "request_id"}`.
    pub fn to_json(&self) -> Value {
        json!({
            "action": self.action.as_str(),
            "sandbox_id": self.sandbox_id,
            "generation": self.generation,
            "request_id": self.request_id,
        })
    }
}

/// What local waits ask of the agent.
pub trait LocalWaitEvents: Send + Sync {
    /// Queue a growth event. Called in order for each sandbox (a wait before
    /// its activate) under the scheduler's lock: it must not block (Python
    /// batches them on one thread, never on the pause's or the thaw's path)
    /// and must not call back into local waits.
    fn growth(&self, event: GrowthEvent);
    /// The thaw fallback (spec §5.2): Python's `POST /v1/sandboxes/{id}/wake
    /// {generation, operation_id}` when the Rust thaw finds a transition in
    /// the way or a runtime that is not RUNNING and live. Python joins the
    /// transition and thaws or restores.
    fn wake(&self, sandbox_id: String, generation: u64, operation_id: String) -> BoxFuture<Result<(), String>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Pause,
    Thaw,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Pause => "pause",
            Kind::Thaw => "thaw",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// This action paused (or thawed) the sandbox.
    Acted,
    /// Nothing to do: already in that state, or no longer a candidate.
    Done,
    /// Exec, file or lifecycle activity holds the sandbox: the next tick decides.
    Busy,
    /// The agent's wake route took the thaw over.
    Delegated,
    Failed(String),
}

/// How the scheduler pauses and thaws (the pause tier in production).
pub(crate) trait Actions: Send + Sync {
    /// The marker exists (observation only).
    fn is_paused(&self, sandbox_id: &str, generation: u64) -> bool;
    fn pause(&self, wait: WaitCandidate, still_waiting: Recheck) -> BoxFuture<Outcome>;
    fn thaw(&self, wait: WaitCandidate) -> BoxFuture<Outcome>;
}

/// Counters of this process's local waits.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    pub packets: AtomicU64,
    pub nflog_drops: AtomicU64,
    pub read_errors: AtomicU64,
    pub pauses: AtomicU64,
    pub thaws: AtomicU64,
    pub busy: AtomicU64,
    pub failures: AtomicU64,
    pub wake_fallbacks: AtomicU64,
    pub growth_waits: AtomicU64,
    pub growth_activates: AtomicU64,
}

impl Stats {
    pub fn add(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct State {
    flows: HashMap<Ipv4Addr, Flow>,
    waits: HashMap<Ipv4Addr, WaitCandidate>,
    busy: HashSet<(String, u64)>,
    logged: HashMap<(String, u64), f64>,
    /// The growth request id of each recorded wait, until its activate.
    requests: HashMap<(String, u64), String>,
    refreshed: f64,
    refresh_logged: f64,
}

pub(crate) struct Scheduler {
    actions: Arc<dyn Actions>,
    candidates: Arc<dyn CandidateSource>,
    events: Arc<dyn LocalWaitEvents>,
    spawn: Spawn,
    clock: Clock,
    permits: Arc<Semaphore>,
    refresh_seconds: f64,
    state: Mutex<State>,
    pub stats: Stats,
    me: Weak<Scheduler>,
}

/// `local-wait-<32 hex>`, as Python's `uuid4().hex` ids (opaque either way).
pub(crate) fn request_id(prefix: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let bytes = crate::guest::random_bytes::<16>().unwrap_or_else(|_| {
        // No randomness: still unique within this process.
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&std::process::id().to_be_bytes().repeat(2));
        bytes[8..].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_be_bytes());
        bytes
    });
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{prefix}-{hex}")
}

impl Scheduler {
    pub fn new(
        actions: Arc<dyn Actions>,
        candidates: Arc<dyn CandidateSource>,
        events: Arc<dyn LocalWaitEvents>,
        spawn: Spawn,
        clock: Clock,
        workers: usize,
        refresh_seconds: f64,
    ) -> Arc<Scheduler> {
        Arc::new_cyclic(|me| Scheduler {
            actions,
            candidates,
            events,
            spawn,
            clock,
            permits: Arc::new(Semaphore::new(workers.max(1))),
            refresh_seconds,
            state: Mutex::new(State { refreshed: f64::NEG_INFINITY, refresh_logged: f64::NEG_INFINITY, ..State::default() }),
            stats: Stats::default(),
            me: me.clone(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn now(&self) -> f64 {
        (self.clock)()
    }

    fn emit(&self, action: GrowthAction, key: &(String, u64), request_id: String) {
        Stats::add(match action {
            GrowthAction::Wait => &self.stats.growth_waits,
            GrowthAction::Activate => &self.stats.growth_activates,
        });
        self.events.growth(GrowthEvent { action, sandbox_id: key.0.clone(), generation: key.1, request_id });
    }

    /// One logged relay packet.
    pub fn observe(&self, packet: &RelayPacket, now: f64) {
        let wait = {
            let mut guard = self.lock();
            let state = &mut *guard;
            let flow = state.flows.entry(packet.guest).or_default();
            if packet.outbound {
                if packet.payload > 0 {
                    flow.last_out = now;
                }
                return;
            }
            if !packet.wakes() {
                return;
            }
            flow.last_in = now;
            let Some(wait) = state.waits.get(&packet.guest).cloned() else { return };
            flow.answered_until = now + ANSWERED_WATCH_SECONDS;
            // The continuation runs from its answer on, however it was thawed
            // (spec §9 #1): an exec's thaw records no activate.
            if let Some(request_id) = state.requests.remove(&wait.key()) {
                self.emit(GrowthAction::Activate, &wait.key(), request_id);
            }
            wait
        };
        if self.actions.is_paused(&wait.sandbox.sandbox_id, wait.sandbox.generation) {
            self.submit(wait, Kind::Thaw);
        }
    }

    /// Its call is answered: hibernating it would strand the answer it holds.
    pub fn answered(&self, sandbox_id: &str, generation: u64, now: f64) -> bool {
        let state = self.lock();
        state.waits.iter().any(|(guest, wait)| {
            wait.sandbox.sandbox_id == sandbox_id
                && wait.sandbox.generation == generation
                && state.flows.get(guest).is_some_and(|flow| flow.last_in >= flow.last_out && flow.answered_until > now)
        })
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        let state = self.lock();
        (state.flows.len(), state.waits.len(), state.busy.len())
    }

    pub(crate) fn submit(&self, wait: WaitCandidate, kind: Kind) -> bool {
        let key = wait.key();
        if !self.lock().busy.insert(key.clone()) {
            return false;
        }
        let Some(me) = self.me.upgrade() else {
            self.lock().busy.remove(&key);
            return false;
        };
        let permits = self.permits.clone();
        (self.spawn)(Box::pin(async move {
            let outcome = {
                let _permit = permits.acquire_owned().await.expect("never closed");
                match kind {
                    Kind::Pause => {
                        let recheck = me.still_waiting(wait.guest);
                        me.actions.pause(wait.clone(), recheck).await
                    }
                    Kind::Thaw => me.actions.thaw(wait.clone()).await,
                }
            };
            me.finish(wait, kind, outcome);
        }));
        true
    }

    fn still_waiting(&self, guest: Ipv4Addr) -> Recheck {
        let me = self.me.clone();
        Box::new(move || me.upgrade().is_some_and(|scheduler| scheduler.lock().flows.get(&guest).is_some_and(Flow::open)))
    }

    fn finish(&self, wait: WaitCandidate, kind: Kind, outcome: Outcome) {
        let key = wait.key();
        match (&outcome, kind) {
            (Outcome::Acted, Kind::Pause) => Stats::add(&self.stats.pauses),
            (Outcome::Acted, Kind::Thaw) => Stats::add(&self.stats.thaws),
            (Outcome::Busy, _) => Stats::add(&self.stats.busy),
            (Outcome::Delegated, _) => Stats::add(&self.stats.wake_fallbacks),
            (Outcome::Failed(_), _) => Stats::add(&self.stats.failures),
            (Outcome::Done, _) => {}
        }
        let now = self.now();
        {
            let mut guard = self.lock();
            let state = &mut *guard;
            if let Outcome::Failed(error) = &outcome {
                let last = state.logged.get(&key).copied().unwrap_or(f64::NEG_INFINITY);
                if now - last >= LOG_EVERY_SECONDS {
                    state.logged.insert(key.clone(), now);
                    eprintln!("ucloud-noded: local wait {} of {}/{} failed: {error}", kind.name(), key.0, key.1);
                }
            }
            state.busy.remove(&key);
            if kind == Kind::Pause && outcome == Outcome::Acted {
                // The guest is blocked on its model call: suspend its growth
                // forecast as a relay park's wait does.
                let request_id = request_id("local-wait");
                self.emit(GrowthAction::Wait, &key, request_id.clone());
                let answered = state.flows.get(&wait.guest).is_some_and(|flow| flow.last_in >= flow.last_out);
                if answered {
                    self.emit(GrowthAction::Activate, &key, request_id); // It raced the pause.
                } else {
                    state.requests.insert(key.clone(), request_id);
                }
            }
        }
        if kind == Kind::Pause && self.actions.is_paused(&key.0, key.1) {
            let answered = {
                let state = self.lock();
                let guest = state.waits.iter().find(|(_, candidate)| candidate.key() == key).map(|(guest, _)| *guest);
                guest.and_then(|guest| state.flows.get(&guest)).is_some_and(|flow| flow.last_in >= flow.last_out)
            };
            if answered {
                self.submit(wait, Kind::Thaw); // The answer raced the pause: thaw now.
            }
        }
    }

    /// The 10 ms policy.
    pub fn tick(&self, now: f64) {
        if now - self.lock().refreshed >= self.refresh_seconds {
            let listed = self.candidates.candidates();
            let mut state = self.lock();
            match listed {
                Ok(listed) => {
                    let waits: HashMap<Ipv4Addr, WaitCandidate> = listed.into_iter().map(|wait| (wait.guest, wait)).collect();
                    let keys: HashSet<(String, u64)> = waits.values().map(WaitCandidate::key).collect();
                    state.flows.retain(|guest, _| waits.contains_key(guest));
                    state.requests.retain(|key, _| keys.contains(key));
                    state.logged.retain(|key, _| keys.contains(key));
                    state.waits = waits;
                }
                Err(error) => {
                    // Keep the last candidates; try again after the interval.
                    if now - state.refresh_logged >= LOG_EVERY_SECONDS {
                        state.refresh_logged = now;
                        eprintln!("ucloud-noded: local wait candidates unavailable: {error}");
                    }
                }
            }
            state.refreshed = now;
        }
        let (answered, pending) = {
            let mut guard = self.lock();
            let state = &mut *guard;
            let mut answered = Vec::new();
            let mut pending = Vec::new();
            for (guest, flow) in &state.flows {
                let Some(wait) = state.waits.get(guest) else { continue };
                if flow.open() {
                    pending.push(wait.clone());
                } else if flow.answered_until > now {
                    answered.push(wait.clone());
                }
            }
            for flow in state.flows.values_mut() {
                if flow.open() {
                    flow.answered_until = 0.0; // The next request went out: that answer was consumed.
                } else {
                    flow.cpu.clear();
                }
            }
            (answered, pending)
        };
        for wait in answered {
            // Never leave an answered call paused; retried every tick.
            if self.actions.is_paused(&wait.sandbox.sandbox_id, wait.sandbox.generation) {
                self.submit(wait, Kind::Thaw);
            }
        }
        for wait in pending {
            let Ok(usage) = cpu_usage_usec(&wait.cpu_stat) else { continue };
            let due = match self.lock().flows.get_mut(&wait.guest) {
                Some(flow) => {
                    flow.sample(now, usage);
                    flow.outstanding(now) && flow.idle(now)
                }
                None => false,
            };
            if due && !self.actions.is_paused(&wait.sandbox.sandbox_id, wait.sandbox.generation) {
                self.submit(wait, Kind::Pause);
            }
        }
    }

    pub fn snapshot(&self) -> Map<String, Value> {
        let (flows, waits, busy) = self.counts();
        let load = |counter: &AtomicU64| Value::from(counter.load(Ordering::Relaxed));
        let stats = &self.stats;
        let mut map = Map::new();
        for (name, value) in [
            ("pauses", load(&stats.pauses)),
            ("thaws", load(&stats.thaws)),
            ("packets", load(&stats.packets)),
            ("nflog_drops", load(&stats.nflog_drops)),
            ("read_errors", load(&stats.read_errors)),
            ("busy_skips", load(&stats.busy)),
            ("failures", load(&stats.failures)),
            ("wake_fallbacks", load(&stats.wake_fallbacks)),
            ("growth_waits", load(&stats.growth_waits)),
            ("growth_activates", load(&stats.growth_activates)),
            ("flows", Value::from(flows)),
            ("candidates", Value::from(waits)),
            ("in_flight", Value::from(busy)),
        ] {
            map.insert(name.to_string(), value);
        }
        map
    }
}
