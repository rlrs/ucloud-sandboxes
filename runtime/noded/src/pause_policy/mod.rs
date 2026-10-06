//! The pause tier's policy (phase 3a; `node_runtime.py` `_reclaim_paused_tick`,
//! `_reclaim_paused`, `_escalate_paused`, `_idle_parking_loop`, `_idle_pause`;
//! `pause_tier.py` `relief_plan`, `ReclaimBudget`; `resident_memory.py`;
//! `warm_park.decide_resident_wait`). `crate::pause` is the mechanism.
//!
//! Two loops:
//! - **Reclaim** (every 250 ms): re-derive the paused table from the markers
//!   (markers are the truth), sample paused sandboxes once a second, decide
//!   from measured pressure and the agent's demand whether paused waits must
//!   give memory back, and start reclaims (at most 2, at one node-wide rate)
//!   and escalations (at most 2) by `relief_plan`. Then write `status.json`.
//! - **Idle pause** (pause tier on, `idle_park_seconds > 0`): pause owned,
//!   parkable, non-managed sandboxes idle that long, under T and A held
//!   exclusively and non-blocking (busy: skip; the next tick decides).
//!
//! An escalation is only a decision here: `Escalator::escalate` asks the
//! agent to hibernate (`POST /internal/v1/pauses/escalate`), which it does
//! through its durable park under its own fence. The daemon's pauses do not
//! advance the activity revision (spec §6.6): nothing the gateway observes
//! changes.

pub mod budget;
pub mod decision;
pub mod engine;
pub mod fence;
pub mod files;
pub mod plan;
pub mod pressure;
pub mod resident;
#[cfg(test)]
pub(crate) mod tests;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::journal::JournalStore;
use crate::pause::{PauseConfig, PauseStats, PauseTier};
use crate::registry::{Phase, Registration, Registry};
use crate::warden::Sandbox;

pub use budget::ReclaimBudget;
pub use decision::{Decision, MemoryBackingCapacity, MemoryDemand, Pressure, decide_resident_wait, swap_room_bytes};
pub use engine::{Engine, IdleOutcome, Job};
pub use fence::PauseFence;
pub use files::{AgentDemand, DemandSource, Status};
pub use plan::{ESCALATION_CONCURRENCY, Key, PausedWait, RECLAIM_CONCURRENCY, relief_plan};
pub use pressure::{PressureSampler, PressureSource};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Monotonic and wall-clock seconds (a seam for tests).
pub trait Clock: Send + Sync {
    fn monotonic(&self) -> f64;
    fn wall(&self) -> f64;
}

pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    pub fn new() -> Self {
        SystemClock { origin: Instant::now() }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn monotonic(&self) -> f64 {
        self.origin.elapsed().as_secs_f64()
    }

    fn wall(&self) -> f64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |elapsed| elapsed.as_secs_f64())
    }
}

/// The recheck a pause runs under the warden flock, on the journal record.
pub type PauseCheck = Box<dyn FnOnce(&Map<String, Value>) -> bool + Send>;

/// The pause mechanism the policy drives (`PauseTier`; fakes in tests).
pub trait PauseMechanism: Send + Sync + 'static {
    fn stats(&self) -> &PauseStats;
    /// Every marker's (sandbox id, generation).
    fn paused_keys(&self) -> std::io::Result<Vec<Key>>;
    fn is_paused(&self, sandbox_id: &str, generation: u64) -> bool;
    /// Paused and no thaw in progress in any process.
    fn settled(&self, sandbox_id: &str, generation: u64) -> bool;
    /// The incarnation's lifecycle journal record (blocking; small).
    fn journal(&self, sandbox_id: &str, generation: u64) -> Result<Option<Map<String, Value>>, String>;
    /// Under the warden flock: pause when the journal is RUNNING, there is no
    /// marker and `check` accepts the record. `Ok(false)`: nothing changed.
    fn pause_checked(&self, sandbox: Sandbox, check: PauseCheck) -> BoxFuture<Result<bool, String>>;
}

impl PauseMechanism for PauseTier {
    fn stats(&self) -> &PauseStats {
        PauseTier::stats(self)
    }

    fn paused_keys(&self) -> std::io::Result<Vec<Key>> {
        PauseTier::paused_keys(self)
    }

    fn is_paused(&self, sandbox_id: &str, generation: u64) -> bool {
        PauseTier::is_paused(self, sandbox_id, generation)
    }

    fn settled(&self, sandbox_id: &str, generation: u64) -> bool {
        PauseTier::settled(self, sandbox_id, generation)
    }

    fn journal(&self, sandbox_id: &str, generation: u64) -> Result<Option<Map<String, Value>>, String> {
        JournalStore::new(self.config().warden.journal_root.clone()).load(sandbox_id, generation).map_err(|error| error.to_string())
    }

    fn pause_checked(&self, sandbox: Sandbox, check: PauseCheck) -> BoxFuture<Result<bool, String>> {
        let tier = self.clone();
        Box::pin(async move {
            if !tier.config().enabled {
                return Err("the pause tier is disabled on this node".into());
            }
            let lock = tier.lock(&sandbox).await.map_err(|error| error.to_string())?;
            let Some(record) = tier.journal_record(&sandbox).await.map_err(|error| error.to_string())? else { return Ok(false) };
            let running = record.get("state").and_then(Value::as_str) == Some("running");
            if !running || tier.is_paused(&sandbox.sandbox_id, sandbox.generation) || !check(&record) {
                return Ok(false);
            }
            tier.pause_locked(&sandbox, &lock, &record).await.map_err(|error| error.to_string())?;
            Ok(true)
        })
    }
}

/// An owned registration as the policy sees it.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub sandbox: Sandbox,
    pub parkable: bool,
    /// `spec.managed_process`.
    pub managed: bool,
}

impl Candidate {
    pub fn from_registration(registration: &Registration) -> Candidate {
        Candidate {
            sandbox: Sandbox {
                sandbox_id: registration.sandbox_id().to_string(),
                generation: registration.sandbox_generation as u64,
                container_id: registration.container_id.clone(),
                bundle: PathBuf::from(&registration.bundle),
                memory_directory: registration.memory_directory.clone(),
                spec_sha256: registration.spec_sha256(),
            },
            parkable: registration.spec.parkable(),
            managed: registration.spec.to_dict().get("managed_process").and_then(Value::as_bool).unwrap_or(false),
        }
    }
}

/// The registry reads the policy needs (`Registry`; fakes in tests).
pub trait Inventory: Send + Sync + 'static {
    /// The durable activity revision (candidates are re-read when it moves).
    fn revision(&self) -> Result<i64, String>;
    /// Every owned registration.
    fn owned(&self) -> Result<Vec<Candidate>, String>;
    /// One sandbox's registration, when owned.
    fn owned_one(&self, sandbox_id: &str) -> Result<Option<Candidate>, String>;
    /// The drain row's `admission_open`.
    fn admission_open(&self) -> Result<bool, String>;
}

impl Inventory for Registry {
    fn revision(&self) -> Result<i64, String> {
        self.activity_revision().map_err(|error| error.to_string())
    }

    fn owned(&self) -> Result<Vec<Candidate>, String> {
        let snapshot = self.snapshot().map_err(|error| error.to_string())?;
        Ok(snapshot.records.iter().filter(|record| record.phase == Phase::Owned).map(|record| Candidate::from_registration(record)).collect())
    }

    fn owned_one(&self, sandbox_id: &str) -> Result<Option<Candidate>, String> {
        let record = self.get(sandbox_id).map_err(|error| error.to_string())?;
        Ok(record.filter(|record| record.phase == Phase::Owned).map(|record| Candidate::from_registration(&record)))
    }

    fn admission_open(&self) -> Result<bool, String> {
        self.load_drain().map(|drain| drain.admission_open).map_err(|error| error.to_string())
    }
}

/// The agent's answer to an escalation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Escalated {
    /// Hibernated (counts `pause_escalations`).
    Parked,
    /// Still running (paused or not): nothing was captured.
    Running,
}

/// Executes the escalation decision: the agent's
/// `POST /internal/v1/pauses/escalate {sandbox_id, generation}`. An error
/// (lost to activity, capture refused, agent unavailable) leaves the
/// sandbox paused; the next tick decides again.
pub trait Escalator: Send + Sync + 'static {
    fn escalate(&self, sandbox: &Sandbox) -> BoxFuture<Result<Escalated, String>>;
}

/// Whether a paused wait's call was answered (local waits); an answered wait
/// is never hibernated.
pub type Answered = Arc<dyn Fn(&str, u64) -> bool + Send + Sync>;

#[derive(Clone, Debug)]
pub struct PolicyConfig {
    /// `status.json` and `agent-demand.json` live in `<state_root>/noded/`.
    pub state_root: PathBuf,
    /// `direct_idle_park_seconds`; 0 or less: no idle pause.
    pub idle_park_seconds: f64,
    pub proc_root: PathBuf,
    pub cgroup_root: PathBuf,
    /// The RAM backing tmpfs (`application_memory_root`), when configured.
    pub memory_backing_root: Option<PathBuf>,
    /// The reclaim tick's period (250 ms).
    pub tick: Duration,
}

impl PolicyConfig {
    /// The roots of the node's pause tier.
    pub fn new(state_root: PathBuf, idle_park_seconds: f64, pause: &PauseConfig) -> PolicyConfig {
        PolicyConfig {
            state_root,
            idle_park_seconds,
            proc_root: pause.warden.proc_root.clone(),
            cgroup_root: pause.cgroup_root.clone(),
            memory_backing_root: pause.warden.application_memory_root.clone(),
            tick: Duration::from_millis(250),
        }
    }

    /// The idle loop's period: `min(1, max(0.05, idle / 4))` seconds.
    pub fn idle_interval(&self) -> Duration {
        Duration::from_secs_f64((self.idle_park_seconds / 4.0).clamp(0.05, 1.0))
    }
}

pub struct PausePolicy;

impl PausePolicy {
    /// Start both loops on the current tokio runtime. `fence` names the
    /// Warden's lock directory (`<runtime_root>/warden-locks`, the exec
    /// fence's); `registry` is the node registry (the daemon owns it).
    pub fn start(
        config: PolicyConfig,
        pause_tier: Arc<dyn PauseMechanism>,
        fence: PauseFence,
        registry: Arc<dyn Inventory>,
        escalator: Arc<dyn Escalator>,
    ) -> PausePolicyHandle {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());
        let pressure = Arc::new(PressureSampler::new(config.proc_root.clone(), config.memory_backing_root.clone(), clock.clone()));
        let inventory = registry.clone();
        let demand = Arc::new(files::AgentDemandSource {
            file: files::DemandFile::new(&config.state_root, clock.clone()),
            // An unreadable drain row bills nothing: only measured pressure acts.
            admission_open: Box::new(move || inventory.admission_open().unwrap_or(false)),
        });
        let deps = engine::Deps {
            mechanism: pause_tier,
            fence,
            inventory: registry,
            escalator,
            budget: ReclaimBudget::node(clock.clone()),
            clock,
            pressure,
            demand,
            paused_reclaim: None,
        };
        Self::start_engine(Arc::new(Engine::new(config, deps)))
    }

    pub(crate) fn start_engine(engine: Arc<Engine>) -> PausePolicyHandle {
        let (stop, stopped) = watch::channel(false);
        let jobs = Arc::new(Mutex::new(Vec::new()));
        let mut loops = vec![tokio::spawn(reclaim_loop(engine.clone(), stopped.clone(), jobs.clone()))];
        if engine.config.idle_park_seconds > 0.0 {
            loops.push(tokio::spawn(idle_loop(engine.clone(), stopped)));
        }
        PausePolicyHandle { engine, stop, loops, jobs }
    }
}

/// Wait `period`; false once stopping.
async fn wait(period: Duration, stopped: &mut watch::Receiver<bool>) -> bool {
    if *stopped.borrow() {
        return false;
    }
    tokio::select! {
        () = tokio::time::sleep(period) => !*stopped.borrow(),
        _ = stopped.changed() => false,
    }
}

type Jobs = Arc<Mutex<Vec<(bool, JoinHandle<()>)>>>;

async fn reclaim_loop(engine: Arc<Engine>, mut stopped: watch::Receiver<bool>, jobs: Jobs) {
    while wait(engine.config.tick, &mut stopped).await {
        let step = engine.clone();
        let Ok(started) = tokio::task::spawn_blocking(move || {
            step.sample_if_due();
            let started = step.tick();
            step.write_status();
            started
        })
        .await
        else {
            continue;
        };
        let mut running = jobs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        running.retain(|(_, job)| !job.is_finished());
        for job in started {
            let engine = engine.clone();
            running.push(match job {
                Job::Reclaim(key, target) => (true, tokio::task::spawn_blocking(move || engine.run_reclaim(&key, target))),
                Job::Escalate(key, sandbox) => (false, tokio::spawn(async move { engine.run_escalation(key, sandbox).await })),
            });
        }
    }
}

async fn idle_loop(engine: Arc<Engine>, mut stopped: watch::Receiver<bool>) {
    while wait(engine.config.idle_interval(), &mut stopped).await {
        engine.idle_pass().await;
    }
}

/// The running policy.
pub struct PausePolicyHandle {
    engine: Arc<Engine>,
    stop: watch::Sender<bool>,
    loops: Vec<JoinHandle<()>>,
    /// In-flight jobs: (blocking reclaim?, task).
    jobs: Jobs,
}

impl PausePolicyHandle {
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// What `status.json` reports next.
    pub fn status(&self) -> Status {
        self.engine.status()
    }

    /// Local waits: a pause just landed, with the relay's predicted wait.
    pub fn record_pause(&self, sandbox_id: &str, generation: u64, expected_wait_seconds: Option<f64>, local_request_id: &str) {
        self.engine.record_pause((sandbox_id.to_string(), generation), expected_wait_seconds, local_request_id);
    }

    /// Local waits: never escalate a wait whose call was answered.
    pub fn set_answered(&self, answered: Answered) {
        *self.engine.answered.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(answered);
    }

    /// Stop both loops; in-flight reclaims stop at their next window (and are
    /// awaited), in-flight escalation requests are abandoned (the agent's
    /// park either completes or leaves the sandbox paused).
    pub async fn stop(self) {
        self.engine.stopped.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = self.stop.send(true);
        for task in self.loops {
            let _ = task.await;
        }
        let jobs = std::mem::take(&mut *self.jobs.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        for (blocking, task) in jobs {
            if blocking {
                let _ = task.await;
            } else {
                task.abort();
            }
        }
    }
}
