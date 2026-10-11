//! The node admission service (docs/admission-port-3c.md §3.1, step 3c.2):
//! the 3c.1 core driven as `DirectSandboxService` drives it.
//!
//! One [`Ledger`] and the managed [`Growth`] beside it sit under one lock
//! (Python's `_capacity_guard`); every change wakes waiters through a watch
//! channel (`_admission_changed`). The startup, restore and publication
//! [`FairCapacity`] pools have their own locks, always taken after the
//! ledger's. Each claim a caller holds is a 128-bit token bound to its holder
//! (noded itself, or one agent session), its owner and its kind.
//!
//! What is Python's, operation by operation:
//! - [`AdmissionService::startup`]: `create_admission` without the request
//!   lock: demand, a startup slot, then capacity, with one deadline;
//! - [`AdmissionService::restore`]: `_restore_admission` from the slot on
//!   (the agent quotes the cost and revalidates the checkpoint after);
//! - [`AdmissionService::observe`]: `_observe_transition` (captures);
//! - [`AdmissionService::publication`]: `_start_storage_publication`'s gate
//!   and lane;
//! - [`AdmissionService::continuation`], [`AdmissionService::launch`],
//!   `bind`, `terminal` and [`AdmissionService::growth_events`]: the managed
//!   growth paths (`_admit_managed_growth`, `_admit_managed_launch`,
//!   `record_local_wait_growth`, `_observe_managed_park`, `_forget_growth`);
//! - [`AdmissionService::close`] / `open`: the drain gate;
//! - [`AdmissionService::snapshot`]: `activity_snapshot`,
//!   `transition_admission_snapshot` and `resident_demand_snapshot` at once;
//! - [`AdmissionService::reclaim_demand`]: what the pause policy bills.
//!
//! Durable growth writes run in detached tasks: a caller that goes away
//! (a client disconnect drops its request future) cannot leave a committed
//! row unpublished, nor a key's turn released while its commit runs. Waits
//! are not detached: dropping a waiting admission gives its place, its slot
//! and its demand back.
//!
//! A new service is **recovering** (§3.5, decision F): it grants nothing
//! until the agent's hello (with the claims it reasserts), or until a grace
//! passes with the agent unreachable.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use super::Owner;
use super::decide::{self, Checks, Decision, Metrics, Node, PHYSICAL_MEMORY_FLOOR_MB, RETRY_SECONDS};
use super::fair::{Entry, FairCapacity};
use super::growth::{self, GROWTH_OBSERVATION_MAX_AGE_SECONDS, Growth, GrowthStore, Observations, Step, Turns};
use super::ledger::{Claim, Cost, DemandSnapshot, Kind, KindSnapshot, Ledger, Resource, Shape};
use crate::pause::prefetch::PREFETCH_MAX_BYTES;
use crate::pause_policy::{Clock, MemoryDemand};
use crate::pipeline::AdmissionConfig;
use crate::registry::{GrowthAction, GrowthIntent, RegistryError};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

const MIB: u64 = 1024 * 1024;
/// Decision F: how long a new service waits for the agent's hello before it
/// asks whether the agent is reachable at all.
pub const RECOVERY_GRACE: Duration = Duration::from_secs(2);
/// `SandboxStartupBusyError` from a startup slot wait.
pub const STARTUP_BUSY: &str = "node startup admission wait deadline exceeded";
/// `SandboxRestoreBusyError` from a restore slot wait.
pub const RESTORE_BUSY: &str = "node restore admission wait deadline exceeded";
pub const RESTORE_LOST: &str = "queued restore generation lost incarnation ownership";
pub const GROWTH_LOST: &str = "queued primary growth lost incarnation";
pub const CONTINUATION_LOST: &str = "managed continuation lost incarnation";
pub const UNAVAILABLE: &str = "direct sandbox is unavailable";
pub const NOT_MANAGED: &str = "sandbox is not a managed-process sandbox";
pub const RECOVERING: &str = "node admission is recovering after a restart";
const PUBLICATION_PROVENANCE: &str = "bounded-storage-backend-stream";

/// Who holds a claim: noded's own operations, or one agent start.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Holder {
    Noded,
    Agent(String),
}

/// The slot pools.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pool {
    Startup,
    Restore,
    Publication,
}

/// Why nothing was granted. Routes give each its wire code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// `SandboxAdmissionClosedError`: the drain gate.
    Closed(String),
    /// `SandboxCapacityUnavailableError`: shape, pressure or projection.
    Capacity(String),
    /// A slot wait ended without a grant: its deadline, a cancel (delete,
    /// drain) or a newer agent session.
    Busy(String),
    /// The owner lost its incarnation, or a durable fence refused.
    Conflict(String),
    /// The registration is gone or not owned.
    Gone(String),
    /// Not a managed-process sandbox.
    NotManaged(String),
    /// `ManagedPrimaryOwnedError(job_id)`.
    PrimaryOwned(String),
    /// A publication waits for the foreground burst, or no lane is free.
    Deferred,
    /// Decision F: no grants until the agent's hello or the grace.
    Recovering,
    /// The agent session has not said hello (or a newer one did).
    HelloRequired,
    Invalid(String),
    /// The registry or a worker failed: nothing was granted.
    Unavailable(String),
}

impl Refusal {
    pub fn message(&self) -> String {
        match self {
            Refusal::Closed(message)
            | Refusal::Capacity(message)
            | Refusal::Busy(message)
            | Refusal::Conflict(message)
            | Refusal::Gone(message)
            | Refusal::NotManaged(message)
            | Refusal::Invalid(message)
            | Refusal::Unavailable(message) => message.clone(),
            Refusal::PrimaryOwned(_) => RegistryError::ManagedPrimaryOwned { job_id: String::new() }.message().to_string(),
            Refusal::Deferred => "storage publication is deferred behind foreground admission".into(),
            Refusal::Recovering => RECOVERING.into(),
            Refusal::HelloRequired => "this agent session has not said hello to noded".into(),
        }
    }
}

impl From<decide::Refusal> for Refusal {
    fn from(refusal: decide::Refusal) -> Self {
        match refusal {
            decide::Refusal::Closed(message) => Refusal::Closed(message),
            decide::Refusal::Capacity(message) => Refusal::Capacity(message),
        }
    }
}

impl From<RegistryError> for Refusal {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::ManagedPrimaryOwned { job_id } => Refusal::PrimaryOwned(job_id),
            RegistryError::Invalid(message) => Refusal::Invalid(message),
            error if error.is_conflict() => Refusal::Conflict(error.message().to_string()),
            error => Refusal::Unavailable(error.to_string()),
        }
    }
}

/// What admission reads about a sandbox ID from the registry.
#[derive(Clone, Debug, PartialEq)]
pub struct OwnerRecord {
    pub generation: u64,
    /// Phase `owned`.
    pub owned: bool,
    /// `spec.managed_process`.
    pub managed: bool,
    /// `spec.memory_mb` (0 when absent).
    pub memory_mb: u64,
}

/// The registry reads admission needs (`Registry`; fakes in tests). Blocking.
pub trait Owners: Send + Sync {
    fn owner(&self, sandbox_id: &str) -> Result<Option<OwnerRecord>, String>;
    /// Every registration's key, whatever its phase (stale intents are pruned).
    fn keys(&self) -> Result<HashSet<Owner>, String>;
}

/// The node's runtime metrics (`runtime_metrics_provider`). Blocking.
pub trait MetricsSource: Send + Sync {
    /// `None`: no fresh metrics, which refuses at once.
    fn sample(&self) -> Option<Metrics>;
}

/// Whether the agent's socket answers at all (decision F's "unreachable").
pub trait AgentProbe: Send + Sync {
    fn reachable(&self) -> BoxFuture<bool>;
}

/// The service's fixed configuration, from the agent's `admission` block.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceConfig {
    pub startup_slots: usize,
    pub restore_slots: usize,
    pub publication_lanes: usize,
    /// `admission_wait_seconds`: the longest slot plus capacity wait.
    pub admission_wait_seconds: f64,
    /// `_active_capacity`; `None` admits on slots alone.
    pub capacity: Option<Shape>,
    /// `application_memory_root` is configured.
    pub ram_backing_configured: bool,
    pub recovery_grace: Duration,
}

impl ServiceConfig {
    /// The block's constants must be the ones this build decides with.
    pub fn from_block(block: &AdmissionConfig, ram_backing_configured: bool) -> Result<ServiceConfig, String> {
        if block.physical_memory_floor_mb != PHYSICAL_MEMORY_FLOOR_MB
            || block.growth_observation_max_age_seconds != GROWTH_OBSERVATION_MAX_AGE_SECONDS
            || block.prefetch_max_bytes != PREFETCH_MAX_BYTES
            || block.retry_seconds != RETRY_SECONDS
        {
            return Err("the agent's admission constants differ from this build's".into());
        }
        if block.max_concurrent_startups == 0 || block.max_concurrent_restores == 0 || block.publication_lanes == 0 {
            return Err("admission pools must have capacity".into());
        }
        if !(block.admission_wait_seconds.is_finite() && block.admission_wait_seconds >= 0.0) {
            return Err("admission_wait_seconds must be a non-negative number".into());
        }
        Ok(ServiceConfig {
            startup_slots: block.max_concurrent_startups,
            restore_slots: block.max_concurrent_restores,
            publication_lanes: block.publication_lanes,
            admission_wait_seconds: block.admission_wait_seconds,
            capacity: block.active_capacity.map(|shape| Shape { vcpu: shape.vcpu, memory_mb: shape.memory_mb, disk_mb: 0 }),
            ram_backing_configured,
            recovery_grace: RECOVERY_GRACE,
        })
    }
}

/// Everything the service reads and writes outside itself.
#[derive(Clone)]
pub struct Sources {
    pub store: Arc<dyn GrowthStore>,
    pub owners: Arc<dyn Owners>,
    pub metrics: Arc<dyn MetricsSource>,
    pub observations: Arc<dyn Observations + Send + Sync>,
    pub clock: Arc<dyn Clock>,
}

/// A held claim.
#[derive(Debug)]
struct Held {
    holder: Holder,
    claim: Claim,
    /// The demand a startup or restore keeps visible until it releases.
    demand: Option<u64>,
    slot: Option<Pool>,
}

struct State {
    ledger: Ledger,
    growth: Growth,
    /// The drain gate.
    admission_open: bool,
    /// The heartbeat's transient revision: CLOCK_MONOTONIC ns at start, +1
    /// per reservation claim and release.
    revision: u64,
    held: HashMap<String, Held>,
    /// The agent session of the last hello.
    agent_session: Option<String>,
    recovering: bool,
}

/// A startup grant and its phases (`phase_timings`).
#[derive(Clone, Debug, PartialEq)]
pub struct StartupGrant {
    pub token: String,
    pub startup_admission_ms: f64,
    pub active_capacity_ms: f64,
}

/// One claim the agent believes it holds (§3.5).
#[derive(Clone, Debug, PartialEq)]
pub struct Reassert {
    pub owner: Owner,
    pub cost: Cost,
    pub requested: Option<Shape>,
    pub token: String,
}

/// `snapshot`: the transient half of the heartbeat, read at once.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    pub admission_open: bool,
    pub transient_revision: u64,
    pub resource_reservations: Vec<(Owner, Shape)>,
    pub transitions: Vec<(Kind, KindSnapshot)>,
    pub resident_demand: DemandSnapshot,
}

/// A growth event's action (`POST /internal/v1/growth/events`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventAction {
    Park,
    Wait,
    Activate,
    Terminal,
    /// Delete: retire the key's intent and provisional claim (no durable step).
    Forget,
}

impl EventAction {
    pub fn parse(text: &str) -> Option<EventAction> {
        Some(match text {
            "park" => EventAction::Park,
            "wait" => EventAction::Wait,
            "activate" => EventAction::Activate,
            "terminal" => EventAction::Terminal,
            "forget" => EventAction::Forget,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub action: EventAction,
    pub owner: Owner,
    pub request_id: String,
    pub job_id: String,
}

pub type EventResult = Result<Option<GrowthIntent>, Refusal>;

/// A queued demand (`_transition_demand`), withdrawn on drop.
struct Demand<'a> {
    service: &'a AdmissionService,
    token: Option<u64>,
}

impl Demand<'_> {
    fn token(&self) -> Option<u64> {
        self.token
    }

    /// The demand now belongs to a held claim (or a detached commit).
    fn keep(mut self) -> u64 {
        self.token.take().expect("kept once")
    }
}

impl Drop for Demand<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.service.lock().ledger.unwait(token);
            self.service.changed();
        }
    }
}

/// A slot taken from a pool, returned on drop.
struct Slot<'a> {
    pool: &'a FairCapacity<Owner>,
    held: bool,
}

impl Slot<'_> {
    fn keep(mut self) {
        self.held = false;
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if self.held {
            let _ = self.pool.release(1);
        }
    }
}

/// One guarded admission (`_active_admission_guard`'s arguments).
struct Admit<'a> {
    holder: &'a Holder,
    owner: &'a Owner,
    /// The shape check's request (vcpu, memory_mb).
    requested: Shape,
    cost: Cost,
    /// Monotonic seconds.
    deadline: f64,
    /// Revalidate ownership each pass; the conflict's message.
    validate: Option<&'static str>,
    /// Growth: requote (bound, kind, request ID) each pass.
    requote: Option<(u64, Kind, &'a str)>,
}

fn new_token() -> String {
    let mut bytes = [0u8; 16];
    let mut filled = 0;
    while filled < bytes.len() {
        // SAFETY: a valid buffer of the stated length, owned by this call.
        let read = unsafe { libc::getrandom(bytes[filled..].as_mut_ptr().cast(), bytes.len() - filled, 0) };
        if read > 0 {
            filled += read as usize;
        } else if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            panic!("getrandom failed: {}", std::io::Error::last_os_error());
        }
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn milliseconds(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

/// Run blocking work (SQLite, /proc) off the reactor.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Result<T, Refusal> {
    tokio::task::spawn_blocking(work).await.map_err(|error| Refusal::Unavailable(format!("admission worker failed: {error}")))
}

/// Await a detached task: dropping the caller does not cancel it.
async fn detached<T: Send + 'static>(work: impl Future<Output = Result<T, Refusal>> + Send + 'static) -> Result<T, Refusal> {
    tokio::spawn(work).await.map_err(|error| Refusal::Unavailable(format!("admission task failed: {error}")))?
}

pub struct AdmissionService {
    config: ServiceConfig,
    state: Mutex<State>,
    changed: watch::Sender<u64>,
    startup: FairCapacity<Owner>,
    restore: FairCapacity<Owner>,
    publication: FairCapacity<Owner>,
    turns: Turns,
    store: Arc<dyn GrowthStore>,
    owners: Arc<dyn Owners>,
    metrics: Arc<dyn MetricsSource>,
    observations: Arc<dyn Observations + Send + Sync>,
    clock: Arc<dyn Clock>,
}

impl AdmissionService {
    /// Load the durable intents, drop those without a registration, and
    /// start recovering. `admission_open` is the drain row's. Blocking.
    pub fn open(config: ServiceConfig, admission_open: bool, sources: Sources) -> Result<Arc<AdmissionService>, String> {
        let Sources { store, owners, metrics, observations, clock } = sources;
        let intents = store.growth_intents().map_err(|error| format!("growth intents: {error}"))?;
        let keys = owners.keys().map_err(|error| format!("registrations: {error}"))?;
        let mut growth = Growth::new(intents.into_iter().filter(|intent| keys.contains(&growth::intent_key(intent))));
        let mut ledger = Ledger::new();
        // No sample yet: every active forecast charges its whole bound (I12).
        growth.refresh_forecasts(&mut ledger, &*observations, clock.monotonic());
        let pool = |capacity: usize| FairCapacity::new(capacity).map_err(|error| error.to_string());
        let state = State {
            ledger,
            growth,
            admission_open,
            revision: (clock.monotonic() * 1e9) as u64,
            held: HashMap::new(),
            agent_session: None,
            recovering: true,
        };
        Ok(Arc::new(AdmissionService {
            startup: pool(config.startup_slots)?,
            restore: pool(config.restore_slots)?,
            publication: pool(config.publication_lanes)?,
            config,
            state: Mutex::new(state),
            changed: watch::channel(0).0,
            turns: Turns::default(),
            store,
            owners,
            metrics,
            observations,
            clock,
        }))
    }

    pub fn config(&self) -> &ServiceConfig {
        &self.config
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn now(&self) -> f64 {
        self.clock.monotonic()
    }

    /// `_admission_changed.notify_all()`.
    fn changed(&self) {
        self.changed.send_modify(|value| *value = value.wrapping_add(1));
    }

    fn pool(&self, pool: Pool) -> &FairCapacity<Owner> {
        match pool {
            Pool::Startup => &self.startup,
            Pool::Restore => &self.restore,
            Pool::Publication => &self.publication,
        }
    }

    fn limits(&self) -> [(Kind, usize); 2] {
        [(Kind::Startup, self.startup.capacity()), (Kind::Restore, self.restore.capacity())]
    }

    fn node(&self, state: &State) -> Node {
        Node {
            admission_open: state.admission_open,
            capacity: self.config.capacity.clone(),
            ram_backing_configured: self.config.ram_backing_configured,
        }
    }

    /// The holder may act now: an agent only under its hello'd session, and
    /// nothing is granted while recovering.
    fn current(state: &State, holder: &Holder, grant: bool) -> Result<(), Refusal> {
        if let Holder::Agent(session) = holder
            && state.agent_session.as_deref() != Some(session.as_str())
        {
            return Err(Refusal::HelloRequired);
        }
        if grant && state.recovering {
            return Err(Refusal::Recovering);
        }
        Ok(())
    }

    fn holds_session(&self, holder: &Holder) -> bool {
        match holder {
            Holder::Noded => true,
            Holder::Agent(session) => self.lock().agent_session.as_deref() == Some(session.as_str()),
        }
    }

    pub fn admission_open(&self) -> bool {
        self.lock().admission_open
    }

    pub fn recovering(&self) -> bool {
        self.lock().recovering
    }

    pub fn agent_session(&self) -> Option<String> {
        self.lock().agent_session.clone()
    }

    /// Tokens held now (tests and status).
    pub fn held(&self) -> usize {
        self.lock().held.len()
    }

    /// Free slots per pool: (startup, restore, publication).
    pub fn available(&self) -> (usize, usize, usize) {
        (self.startup.available(), self.restore.available(), self.publication.available())
    }

    /// The in-memory intent for `owner` (tests and status).
    pub fn intent(&self, owner: &Owner) -> Option<GrowthIntent> {
        self.lock().growth.intent(owner).cloned()
    }

    /// The forecast charged for `owner` (tests and status).
    pub fn forecast(&self, owner: &Owner) -> Option<Cost> {
        self.lock().ledger.growth_forecast(owner).cloned()
    }

    // Registry and observation reads, off the reactor.

    async fn owner_record(&self, sandbox_id: &str) -> Result<Option<OwnerRecord>, Refusal> {
        let (owners, sandbox_id) = (self.owners.clone(), sandbox_id.to_string());
        blocking(move || owners.owner(&sandbox_id)).await?.map_err(Refusal::Unavailable)
    }

    /// Owned at this generation, or `Conflict(message)`.
    async fn validate(&self, owner: &Owner, message: &str) -> Result<OwnerRecord, Refusal> {
        match self.owner_record(&owner.0).await? {
            Some(record) if record.owned && record.generation == owner.1 => Ok(record),
            _ => Err(Refusal::Conflict(message.into())),
        }
    }

    /// `_require_managed_generation`.
    async fn managed_generation(&self, owner: &Owner) -> Result<OwnerRecord, Refusal> {
        let record = match self.owner_record(&owner.0).await? {
            Some(record) if record.owned => record,
            _ => return Err(Refusal::Gone(UNAVAILABLE.into())),
        };
        if !record.managed {
            return Err(Refusal::NotManaged(NOT_MANAGED.into()));
        }
        if record.generation != owner.1 {
            return Err(Refusal::Conflict(format!("sandbox {} is generation {}, not {}", owner.0, record.generation, owner.1)));
        }
        Ok(record)
    }

    async fn sample(&self) -> Option<Metrics> {
        let metrics = self.metrics.clone();
        blocking(move || metrics.sample()).await.ok().flatten()
    }

    /// `_growth_cost` with a fresh placement and the latest sample.
    async fn quote(&self, owner: &Owner, bound: u64, kind: Kind, request_id: &str) -> Cost {
        let (observations, key) = (self.observations.clone(), owner.clone());
        let _ = blocking(move || observations.reload_mode(&key)).await;
        let sample = self.observations.historical(owner);
        growth::growth_cost(self.observations.memory_mode(owner), bound, kind, request_id, sample.as_ref(), self.now())
    }

    // The ledger's pieces.

    fn demand(&self, owner: &Owner, cost: Cost) -> Demand<'_> {
        let token = self.lock().ledger.wait(owner.clone(), cost);
        Demand { service: self, token: Some(token) }
    }

    /// Queue for one slot until `deadline`. `None`: the deadline passed, the
    /// owner's (or every) waiter was cancelled, or the holder's session ended.
    async fn slot(&self, pool: Pool, owner: &Owner, holder: &Holder, deadline: f64) -> Option<Slot<'_>> {
        let capacity = self.pool(pool);
        let mut changed = self.changed.subscribe();
        let waiter = match capacity.enter(1, Some(owner.clone()), true) {
            Ok(Entry::Granted) => return Some(Slot { pool: capacity, held: true }),
            Ok(Entry::Queued(waiter)) => waiter,
            Ok(Entry::Refused) | Err(_) => return None,
        };
        loop {
            let remaining = deadline - self.now();
            if remaining <= 0.0 || !self.holds_session(holder) {
                break;
            }
            tokio::select! {
                () = waiter.ready() => break,
                () = tokio::time::sleep(Duration::from_secs_f64(remaining)) => break,
                result = changed.changed() => if result.is_err() { break },
            }
        }
        waiter.finish().then_some(Slot { pool: capacity, held: true })
    }

    /// Publish a claim under the ledger lock and hand out its token.
    fn hold(state: &mut State, holder: &Holder, owner: &Owner, cost: &Cost, requested: Option<Shape>, demand: Option<u64>, slot: Option<Pool>) -> String {
        let claim = state.ledger.claim(owner.clone(), cost.clone(), requested);
        if claim.requested.is_some() {
            state.revision += 1;
        }
        let token = new_token();
        state.held.insert(token.clone(), Held { holder: holder.clone(), claim, demand, slot });
        token
    }

    fn unhold(&self, state: &mut State, held: Held) {
        let _ = state.ledger.release(&held.claim);
        if let Some(token) = held.demand {
            state.ledger.unwait(token);
        }
        if let Some(pool) = held.slot {
            let _ = self.pool(pool).release(1);
        }
        if held.claim.requested.is_some() {
            state.revision += 1;
        }
    }

    /// `_active_admission_guard`: sample outside the lock, decide inside it,
    /// and wait on changes while the decision says so. `grant` runs under the
    /// lock with the admitted cost.
    async fn admit<T>(&self, admit: Admit<'_>, grant: impl FnOnce(&mut State, &Cost) -> T) -> Result<T, Refusal> {
        let mut changed = self.changed.subscribe();
        let mut grant = Some(grant);
        let mut request = decide::Request {
            node: Node { admission_open: false, capacity: None, ram_backing_configured: false },
            owner: admit.owner.clone(),
            requested: admit.requested.clone(),
            cost: Some(admit.cost.clone()),
            checks: Checks::default(),
            deadline: admit.deadline,
            now: self.now(),
            previous_error: None,
        };
        loop {
            {
                let state = self.lock();
                Self::current(&state, admit.holder, true)?;
                request.node = self.node(&state);
                request.now = self.now();
                decide::gate(&request)?;
            }
            // Never hold the lock while /proc or the registry is read.
            let metrics = if request.node.capacity.is_some() { self.sample().await } else { None };
            if let Some(message) = admit.validate {
                self.validate(admit.owner, message).await?;
            }
            if let Some((bound, kind, request_id)) = admit.requote {
                request.cost = Some(self.quote(admit.owner, bound, kind, request_id).await);
            }
            let seconds = {
                let mut guard = self.lock();
                let state = &mut *guard;
                Self::current(state, admit.holder, true)?;
                request.node = self.node(state);
                if let (Some(cost), Some(_), Some(_)) = (&request.cost, &request.node.capacity, &metrics) {
                    // The projections charge current forecasts and this
                    // owner's requoted demand.
                    state.growth.current_forecasts(&mut state.ledger, &*self.observations, self.now());
                    if admit.requote.is_some() {
                        state.ledger.refresh_wait_cost(admit.owner, cost);
                    }
                }
                request.now = self.now();
                match decide::decide(metrics.as_ref(), &state.ledger, &request) {
                    Decision::Grant => {
                        let cost = request.cost.clone().expect("admissions carry a cost");
                        let grant = grant.take().expect("granted once");
                        return Ok(grant(state, &cost));
                    }
                    Decision::Refuse(refusal) => return Err(refusal.into()),
                    Decision::Wait { reason, seconds } => {
                        // Keep the demand visible until headroom returns;
                        // delete and drain can still interrupt.
                        request.previous_error = Some(reason);
                        changed.borrow_and_update();
                        seconds
                    }
                }
            };
            let _ = tokio::time::timeout(Duration::from_secs_f64(seconds), changed.changed()).await;
        }
    }

    // Creates, restores, captures and publications.

    /// `create_admission` minus the request lock: demand, a startup slot,
    /// then capacity, sharing one deadline. `wait_seconds` shortens the
    /// configured wait (`CREATE_ADMISSION_WAIT`).
    pub async fn startup(&self, holder: &Holder, owner: &Owner, requested: Shape, managed: bool, wait_seconds: Option<f64>) -> Result<StartupGrant, Refusal> {
        Self::current(&self.lock(), holder, true)?;
        let cost = growth::startup_cost(self.observations.memory_mode(owner), requested.memory_mb, managed);
        let demand = self.demand(owner, cost.clone());
        if !self.lock().admission_open {
            return Err(Refusal::Closed(decide::CLOSED.into()));
        }
        let configured = self.config.admission_wait_seconds;
        let wait = wait_seconds.map_or(configured, |wait| wait.min(configured)).max(0.0);
        let deadline = self.now() + wait;
        let started = Instant::now();
        let slot = self.slot(Pool::Startup, owner, holder, deadline).await.ok_or_else(|| Refusal::Busy(STARTUP_BUSY.into()))?;
        if !self.lock().admission_open {
            return Err(Refusal::Closed(decide::CLOSED.into()));
        }
        let startup_admission_ms = milliseconds(started);
        let started = Instant::now();
        let shape = Shape { vcpu: requested.vcpu, memory_mb: requested.memory_mb, disk_mb: 0 };
        let admit = Admit { holder, owner, requested: shape, cost, deadline, validate: None, requote: None };
        let demand_token = demand.token();
        let token = self
            .admit(admit, |state, cost| Self::hold(state, holder, owner, cost, Some(requested), demand_token, Some(Pool::Startup)))
            .await?;
        demand.keep();
        slot.keep();
        Ok(StartupGrant { token, startup_admission_ms, active_capacity_ms: milliseconds(started) })
    }

    /// `_restore_admission` from the slot on: the agent quoted `cost`
    /// (kind restore) and revalidates the checkpoint after the grant.
    /// `budget_seconds` is what remains of its deadline.
    pub async fn restore(&self, holder: &Holder, owner: &Owner, requested: Shape, cost: Cost, budget_seconds: f64) -> Result<String, Refusal> {
        Self::current(&self.lock(), holder, true)?;
        let demand = self.demand(owner, cost.clone());
        let deadline = self.now() + budget_seconds.max(0.0);
        let slot = self.slot(Pool::Restore, owner, holder, deadline).await.ok_or_else(|| Refusal::Busy(RESTORE_BUSY.into()))?;
        let shape = Shape { vcpu: requested.vcpu, memory_mb: requested.memory_mb, disk_mb: 0 };
        let admit = Admit { holder, owner, requested: shape, cost, deadline, validate: Some(RESTORE_LOST), requote: None };
        let demand_token = demand.token();
        let token = self
            .admit(admit, |state, cost| Self::hold(state, holder, owner, cost, Some(requested), demand_token, Some(Pool::Restore)))
            .await?;
        demand.keep();
        slot.keep();
        Ok(token)
    }

    /// `_observe_transition`: a capture is accounted, never queued or
    /// refused (I16).
    pub fn observe(&self, holder: &Holder, owner: &Owner, cost: Cost) -> Result<String, Refusal> {
        let mut state = self.lock();
        Self::current(&state, holder, false)?;
        Ok(Self::hold(&mut state, holder, owner, &cost, None, None, None))
    }

    /// `_start_storage_publication`'s gate and lane, atomically: one
    /// maintenance publication keeps progressing through a foreground burst,
    /// but no wave starts ahead of queued wakes.
    pub fn publication(&self, holder: &Holder, owner: &Owner) -> Result<String, Refusal> {
        let mut state = self.lock();
        Self::current(&state, holder, true)?;
        if state.ledger.foreground_waiting() && state.ledger.active_count(Kind::Publication) > 0 {
            return Err(Refusal::Deferred);
        }
        if !self.publication.try_acquire(1, Some(owner.clone())).unwrap_or(false) {
            return Err(Refusal::Deferred);
        }
        let cost = Cost { provenance: PUBLICATION_PROVENANCE.into(), ..Cost::new(Kind::Publication, None) };
        Ok(Self::hold(&mut state, holder, owner, &cost, None, None, Some(Pool::Publication)))
    }

    /// Release a held claim, its demand and its slot. Idempotent: `false`
    /// when this holder holds no such token (released, or never its).
    pub fn release(&self, token: &str, holder: &Holder) -> bool {
        let mut state = self.lock();
        if state.held.get(token).is_none_or(|held| &held.holder != holder) {
            return false;
        }
        let held = state.held.remove(token).expect("checked above");
        self.unhold(&mut state, held);
        drop(state);
        self.changed();
        true
    }

    /// Delete: wake this owner's queued startups and restores (a granted
    /// slot is never revoked) and every capacity waiter, which revalidates.
    pub fn cancel(&self, owner: &Owner) -> usize {
        let cancelled = self.startup.cancel_waiters(Some(owner)) + self.restore.cancel_waiters(Some(owner));
        self.changed();
        cancelled
    }

    /// Drain: once this returns, every admitted create and restore is held
    /// (in the snapshot) or released, and nothing new can be granted.
    /// Returns (admission open, transient revision).
    pub fn close(&self) -> (bool, u64) {
        self.lock().admission_open = false;
        self.changed();
        self.startup.cancel_waiters(None);
        self.restore.cancel_waiters(None);
        let state = self.lock();
        (state.admission_open, state.revision)
    }

    pub fn open_admission(&self) -> (bool, u64) {
        let mut state = self.lock();
        state.admission_open = true;
        let answer = (state.admission_open, state.revision);
        drop(state);
        self.changed();
        answer
    }

    /// The heartbeat's transient view; refused while recovering, so a
    /// heartbeat never reports a restarted noded's empty ledger.
    pub fn snapshot(&self) -> Result<Snapshot, Refusal> {
        let now = self.now();
        let mut guard = self.lock();
        if guard.recovering {
            return Err(Refusal::Recovering);
        }
        let state = &mut *guard;
        state.growth.current_forecasts(&mut state.ledger, &*self.observations, now);
        Ok(Snapshot {
            admission_open: state.admission_open,
            transient_revision: state.revision,
            resource_reservations: state.ledger.resource_reservations(),
            transitions: state.ledger.snapshot(),
            resident_demand: state.ledger.demand_snapshot(&self.limits()),
        })
    }

    /// `reclaim_demand`: admitted work plus the next eligible FIFO owner;
    /// closed admission bills nothing.
    pub fn reclaim_demand(&self) -> MemoryDemand {
        let now = self.now();
        let mut guard = self.lock();
        if !guard.admission_open {
            return MemoryDemand::default();
        }
        let state = &mut *guard;
        state.growth.current_forecasts(&mut state.ledger, &*self.observations, now);
        let limits = self.limits();
        MemoryDemand {
            physical_bytes: state.ledger.next_memory_demands(&limits, Resource::Memory),
            ram_backing_bytes: state.ledger.next_memory_demands(&limits, Resource::RamBacking),
        }
    }

    // Sessions and recovery.

    /// The agent's hello (§3.5): drop every claim held under any other agent
    /// session (their waiters see the change and give up), accept the claims
    /// this session reasserts, and end recovery. Returns (claims dropped,
    /// claims reasserted).
    pub fn hello(&self, session: &str, reassert: Vec<Reassert>) -> (usize, usize) {
        let mut guard = self.lock();
        let state = &mut *guard;
        let mut dropped = 0;
        if state.agent_session.as_deref() != Some(session) {
            let stale: Vec<String> = state
                .held
                .iter()
                .filter(|(_, held)| matches!(&held.holder, Holder::Agent(other) if other != session))
                .map(|(token, _)| token.clone())
                .collect();
            for token in stale {
                let held = state.held.remove(&token).expect("listed above");
                self.unhold(state, held);
                dropped += 1;
            }
            state.agent_session = Some(session.to_string());
        }
        let holder = Holder::Agent(session.to_string());
        let mut reasserted = 0;
        for item in reassert {
            if state.held.contains_key(&item.token) {
                continue;
            }
            // Already running: accepted unconditionally, with a slot while
            // the pool has one.
            let pool = match item.cost.kind {
                Kind::Startup => Some(Pool::Startup),
                Kind::Restore => Some(Pool::Restore),
                Kind::Publication => Some(Pool::Publication),
                Kind::Capture => None,
            };
            let slot = pool.filter(|pool| self.pool(*pool).try_acquire(1, Some(item.owner.clone())).unwrap_or(false));
            let claim = state.ledger.claim(item.owner, item.cost, item.requested);
            if claim.requested.is_some() {
                state.revision += 1;
            }
            state.held.insert(item.token, Held { holder: holder.clone(), claim, demand: None, slot });
            reasserted += 1;
        }
        let ended = std::mem::replace(&mut state.recovering, false);
        drop(guard);
        if ended {
            eprintln!("ucloud-noded: admission recovered with agent session {session} ({reasserted} claims reasserted)");
        }
        self.changed();
        (dropped, reasserted)
    }

    fn end_recovery(&self) -> bool {
        let ended = std::mem::replace(&mut self.lock().recovering, false);
        self.changed();
        ended
    }

    /// Decision F: after the grace, recovery ends without a hello only when
    /// the agent is unreachable (it is down too, so its claims are gone);
    /// while it answers, its hello is awaited.
    pub fn start_recovery(self: &Arc<Self>, probe: Option<Arc<dyn AgentProbe>>) {
        let service = Arc::downgrade(self);
        let grace = self.config.recovery_grace;
        tokio::spawn(async move {
            let mut waiting_logged = false;
            loop {
                tokio::time::sleep(grace).await;
                let Some(current) = service.upgrade() else { return };
                if !current.recovering() {
                    return;
                }
                if let Some(probe) = &probe
                    && probe.reachable().await
                {
                    if !waiting_logged {
                        eprintln!("ucloud-noded: admission is recovering: waiting for the agent's hello");
                        waiting_logged = true;
                    }
                    continue;
                }
                if current.end_recovery() {
                    eprintln!("ucloud-noded: admission recovered without a hello: the agent is unreachable");
                }
                return;
            }
        });
    }

    /// After each sampler pass (blocking context): drop intents whose
    /// registration is gone and credit the pass's samples, as
    /// `refresh_resident_memory` does.
    pub fn after_resident_pass(&self) {
        let keys = self.owners.keys().ok();
        let now = self.now();
        let mut guard = self.lock();
        let state = &mut *guard;
        if let Some(keys) = keys {
            state.growth.retain(|key| keys.contains(key), &mut state.ledger, &*self.observations, now);
        }
        state.growth.refresh_forecasts(&mut state.ledger, &*self.observations, now);
        drop(guard);
        self.changed();
    }

    // Managed growth.

    /// `_record_growth_intent` under the key's turn (the caller's).
    async fn commit(&self, step: Step) -> Result<Option<GrowthIntent>, Refusal> {
        let (store, key) = (self.store.clone(), step.key.clone());
        let intent = blocking(move || store.growth_intent(&step)).await??;
        let now = self.now();
        let mut guard = self.lock();
        let state = &mut *guard;
        state.growth.record(&key, intent.clone(), &mut state.ledger, &*self.observations, now);
        drop(guard);
        self.changed();
        Ok(intent)
    }

    /// One durable transition in its turn, detached from the caller.
    pub async fn transition(self: &Arc<Self>, step: Step) -> Result<Option<GrowthIntent>, Refusal> {
        let service = self.clone();
        detached(async move {
            let _turn = service.turns.take(&step.key).await;
            service.commit(step).await
        })
        .await
    }

    /// `_admit_managed_growth`: the covered check under the key's turn, then
    /// demand and the guarded admission with requotes, a provisional charge
    /// before the lock is released, and the durable `activate` (which commits
    /// a continuation's wake fence with it).
    #[allow(clippy::too_many_arguments)]
    async fn admit_growth(
        self: &Arc<Self>,
        holder: &Holder,
        owner: &Owner,
        record: &OwnerRecord,
        request_id: &str,
        startup: bool,
        launch: (String, String),
        deadline: f64,
    ) -> Result<Option<GrowthIntent>, Refusal> {
        let kind = if startup { Kind::Startup } else { Kind::Restore };
        let bound = record.memory_mb * MIB;
        let (service, key, request) = (self.clone(), owner.clone(), request_id.to_string());
        let covered = detached(async move {
            // Serialized with wait and park updates: a safe wait cannot
            // release this exposure between the check and its fence.
            let _turn = service.turns.take(&key).await;
            let previous = service.lock().growth.covered(&key, startup, &request).cloned();
            if previous.is_some() && !request.is_empty() {
                let (store, key) = (service.store.clone(), key.clone());
                blocking(move || store.relay_wake_fence(&key, &request)).await??;
            }
            Ok(previous)
        })
        .await?;
        if covered.is_some() {
            return Ok(covered);
        }
        // Growth reserves bytes, not slots: the memory guard is the bound.
        let cost = self.quote(owner, bound, kind, request_id).await;
        let demand = self.demand(owner, cost.clone());
        let shape = Shape { vcpu: 0.0, memory_mb: record.memory_mb, disk_mb: 0 };
        let admit = Admit { holder, owner, requested: shape, cost, deadline, validate: Some(GROWTH_LOST), requote: Some((bound, kind, request_id)) };
        let observations = self.observations.clone();
        let now = self.now();
        self.admit(admit, |state, cost| {
            // Charged before the lock is released, so a concurrent admission
            // cannot spend these bytes while the commit fsyncs.
            state.growth.charge_provisional(owner, cost.clone(), &mut state.ledger, &*observations, now);
        })
        .await?;
        let demand = demand.keep();
        let (service, key, request) = (self.clone(), owner.clone(), request_id.to_string());
        detached(async move {
            let step = Step::new(&key, GrowthAction::Activate).request(&request).job(&launch.0, &launch.1);
            let result = {
                let _turn = service.turns.take(&key).await;
                service.commit(step).await
            };
            let now = service.now();
            let mut guard = service.lock();
            let state = &mut *guard;
            state.growth.clear_provisional(&key, &mut state.ledger, &*service.observations, now);
            state.ledger.unwait(demand);
            drop(guard);
            service.changed();
            result
        })
        .await
    }

    /// `admit_managed_continuation` after the agent's lock probe: reserve a
    /// relay continuation's growth before its wake is acknowledged.
    /// `Ok(None)`: not a managed-process sandbox.
    pub async fn continuation(self: &Arc<Self>, holder: &Holder, owner: &Owner, request_id: &str, budget_seconds: f64) -> Result<Option<GrowthIntent>, Refusal> {
        Self::current(&self.lock(), holder, true)?;
        let record = match self.owner_record(&owner.0).await? {
            Some(record) if record.owned => record,
            _ => return Err(Refusal::Conflict(UNAVAILABLE.into())),
        };
        if record.generation != owner.1 {
            return Err(Refusal::Conflict(CONTINUATION_LOST.into()));
        }
        if !record.managed {
            return Ok(None);
        }
        let deadline = self.now() + budget_seconds.max(0.0);
        self.admit_growth(holder, owner, &record, request_id, false, (String::new(), String::new()), deadline).await
    }

    /// `_admit_managed_launch` for noded's own job start: commit the launch
    /// before the ambiguous dispatch and admit a queued one. Returns the
    /// committed intent's job ID (empty for an imported primary, which the
    /// caller binds after the dispatch).
    pub async fn launch(self: &Arc<Self>, owner: &Owner, job_id: &str, launch_sha256: &str) -> Result<String, Refusal> {
        let holder = Holder::Noded;
        Self::current(&self.lock(), &holder, true)?;
        let record = self.managed_generation(owner).await?;
        let intent = self.transition(Step::new(owner, GrowthAction::Launch).job(job_id, launch_sha256)).await?;
        let Some(intent) = intent else { return Err(Refusal::Unavailable("the launch committed no intent".into())) };
        if intent.phase == "queued" {
            let deadline = self.now() + self.config.admission_wait_seconds;
            let launch = (job_id.to_string(), launch_sha256.to_string());
            let admitted = match self.admit_growth(&holder, owner, &record, "", true, launch, deadline).await {
                // Not dispatched: the bounded safe-retry contract.
                Err(Refusal::Capacity(message)) => return Err(Refusal::Busy(message)),
                other => other?,
            };
            // Another launch may replace a queued one meanwhile; an active
            // identity cannot be replaced, so a match fences the dispatch.
            match admitted {
                Some(admitted) if admitted.phase != "queued" && admitted.job_id == job_id && admitted.launch_sha256 == launch_sha256 => {}
                other => return Err(Refusal::PrimaryOwned(other.map(|admitted| admitted.job_id).unwrap_or_default())),
            }
        }
        Ok(intent.job_id)
    }

    /// `bind_managed_launch`: an imported primary's identity after dispatch.
    pub async fn bind(self: &Arc<Self>, owner: &Owner, job_id: &str, launch_sha256: &str) -> Result<Option<GrowthIntent>, Refusal> {
        self.managed_generation(owner).await?;
        self.transition(Step::new(owner, GrowthAction::Bind).job(job_id, launch_sha256)).await
    }

    /// `observe_managed_terminal`: the first terminal record of this primary.
    pub async fn terminal(self: &Arc<Self>, owner: &Owner, job_id: &str) -> Result<Option<GrowthIntent>, Refusal> {
        self.managed_generation(owner).await?;
        let mut step = Step::new(owner, GrowthAction::Terminal);
        step.job_id = job_id.to_string();
        self.transition(step).await
    }

    /// The remaining growth writers as one batch (`record_local_wait_growth`
    /// generalized): turns in key order, one commit, each item failing alone.
    /// A registration that is missing or not owned fails its item; a
    /// sandbox that is not managed is skipped (`Ok(None)`). `forget` retires
    /// the key in memory only (delete); `park` forgets its samples first.
    pub async fn growth_events(self: &Arc<Self>, events: Vec<Event>) -> Result<Vec<EventResult>, Refusal> {
        let service = self.clone();
        detached(async move { Ok(service.apply_events(events).await) }).await
    }

    async fn apply_events(&self, events: Vec<Event>) -> Vec<EventResult> {
        let ids: Vec<String> = events.iter().filter(|event| event.action != EventAction::Forget).map(|event| event.owner.0.clone()).collect();
        let owners = self.owners.clone();
        let records: HashMap<String, Result<Option<OwnerRecord>, String>> = match blocking(move || {
            let mut records = HashMap::new();
            for id in ids {
                if let std::collections::hash_map::Entry::Vacant(entry) = records.entry(id) {
                    let record = owners.owner(entry.key());
                    entry.insert(record);
                }
            }
            records
        })
        .await
        {
            Ok(records) => records,
            Err(refusal) => return events.iter().map(|_| Err(refusal.clone())).collect(),
        };
        let mut results: Vec<EventResult> = events.iter().map(|_| Ok(None)).collect();
        let mut steps: Vec<(usize, Step)> = Vec::new();
        let mut forgets: Vec<Owner> = Vec::new();
        for (index, event) in events.iter().enumerate() {
            let action = match event.action {
                EventAction::Forget => {
                    forgets.push(event.owner.clone());
                    continue;
                }
                EventAction::Park => GrowthAction::Park,
                EventAction::Wait => GrowthAction::Wait,
                EventAction::Activate => GrowthAction::Activate,
                EventAction::Terminal => GrowthAction::Terminal,
            };
            match records.get(&event.owner.0) {
                Some(Ok(Some(record))) if record.owned => {
                    if record.managed {
                        let mut step = Step::new(&event.owner, action).request(&event.request_id);
                        step.job_id = event.job_id.clone();
                        steps.push((index, step));
                    }
                }
                Some(Err(message)) => results[index] = Err(Refusal::Unavailable(message.clone())),
                _ => results[index] = Err(Refusal::Gone(UNAVAILABLE.into())),
            }
        }
        let keys: Vec<Owner> = steps.iter().map(|(_, step)| step.key.clone()).chain(forgets.iter().cloned()).collect();
        let _turns = self.turns.take_all(keys.iter()).await;
        for (_, step) in &steps {
            if step.action == GrowthAction::Park {
                self.observations.forget(&step.key);
            }
        }
        let committed = if steps.is_empty() {
            Ok(Vec::new())
        } else {
            let (store, batch) = (self.store.clone(), steps.iter().map(|(_, step)| step.clone()).collect::<Vec<_>>());
            match blocking(move || store.growth_steps(&batch)).await {
                Ok(Ok(items)) => Ok(items),
                Ok(Err(error)) => Err(Refusal::from(error)),
                Err(refusal) => Err(refusal),
            }
        };
        let now = self.now();
        let mut guard = self.lock();
        let state = &mut *guard;
        match committed {
            Ok(items) => {
                for ((index, step), item) in steps.iter().zip(items) {
                    match item {
                        Ok(intent) => {
                            state.growth.record(&step.key, intent.clone(), &mut state.ledger, &*self.observations, now);
                            results[*index] = Ok(intent);
                        }
                        Err(error) => {
                            state.growth.record(&step.key, None, &mut state.ledger, &*self.observations, now);
                            results[*index] = Err(error.into());
                        }
                    }
                }
            }
            Err(refusal) => {
                for (index, step) in &steps {
                    state.growth.record(&step.key, None, &mut state.ledger, &*self.observations, now);
                    results[*index] = Err(refusal.clone());
                }
            }
        }
        for key in &forgets {
            // Waits out any in-flight commit (its turn), so it cannot
            // republish a deleted owner.
            state.growth.forget(key, &mut state.ledger, &*self.observations, now);
        }
        drop(guard);
        self.changed();
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_128_bit_hex_and_distinct() {
        let (first, second) = (new_token(), new_token());
        assert_eq!(first.len(), 32);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
        assert_ne!(first, second);
    }
}
