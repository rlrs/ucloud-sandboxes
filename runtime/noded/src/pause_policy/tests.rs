//! The policy against fakes for time, the pause mechanism, the registry and
//! the agent, with a fake /proc/meminfo under the real pressure sampler,
//! ported from tests/test_pause_tier.py `PauseRuntimeTests` and
//! tests/test_node_runtime.py's idle-park tests; one test runs the real
//! `PauseTier` against a fake runsc.

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};

use super::budget::tests::FakeNice;
use super::engine::{Deps, PausedReclaim};
use super::files::DemandSource;
use super::plan::{MAX_STALLS, RECLAIM_CONCURRENCY};
use super::resident::ReclaimResult;
use super::*;
use crate::pause::tests::TempDir;
use crate::pause::{Counter, PauseStats};

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// Monotonic and wall-clock time that move only when told.
pub(crate) struct FakeClock {
    now: Mutex<(f64, f64)>,
}

impl FakeClock {
    pub fn new() -> Arc<FakeClock> {
        Self::at(0.0)
    }

    pub fn at(monotonic: f64) -> Arc<FakeClock> {
        Arc::new(FakeClock { now: Mutex::new((monotonic, 1_000_000.0)) })
    }

    /// Both clocks move.
    pub fn advance(&self, seconds: f64) {
        let mut now = self.now.lock().unwrap();
        now.0 += seconds;
        now.1 += seconds;
    }

    pub fn set_wall(&self, wall: f64) {
        self.now.lock().unwrap().1 = wall;
    }
}

impl Clock for FakeClock {
    fn monotonic(&self) -> f64 {
        self.now.lock().unwrap().0
    }

    fn wall(&self) -> f64 {
        self.now.lock().unwrap().1
    }
}

/// The mechanism: markers in a set, journals in a map, pauses recorded.
#[derive(Default)]
struct FakeTier {
    stats: PauseStats,
    paused: Mutex<HashSet<Key>>,
    thawing: Mutex<HashSet<Key>>,
    journals: Mutex<HashMap<Key, Map<String, Value>>>,
    pauses: Mutex<Vec<Key>>,
    fail_pause: Mutex<bool>,
}

impl FakeTier {
    fn mark(&self, key: Key) {
        self.paused.lock().unwrap().insert(key);
    }

    fn markers(&self) -> HashSet<Key> {
        self.paused.lock().unwrap().clone()
    }
}

impl PauseMechanism for FakeTier {
    fn stats(&self) -> &PauseStats {
        &self.stats
    }

    fn paused_keys(&self) -> std::io::Result<Vec<Key>> {
        let mut keys: Vec<Key> = self.markers().into_iter().collect();
        keys.sort();
        Ok(keys)
    }

    fn is_paused(&self, sandbox_id: &str, generation: u64) -> bool {
        self.paused.lock().unwrap().contains(&(sandbox_id.to_string(), generation))
    }

    fn settled(&self, sandbox_id: &str, generation: u64) -> bool {
        let key = (sandbox_id.to_string(), generation);
        self.is_paused(sandbox_id, generation) && !self.thawing.lock().unwrap().contains(&key)
    }

    fn journal(&self, sandbox_id: &str, generation: u64) -> Result<Option<Map<String, Value>>, String> {
        Ok(self.journals.lock().unwrap().get(&(sandbox_id.to_string(), generation)).cloned())
    }

    fn pause_checked(&self, sandbox: Sandbox, check: PauseCheck) -> BoxFuture<Result<bool, String>> {
        let key = (sandbox.sandbox_id.clone(), sandbox.generation);
        let record = self.journals.lock().unwrap().get(&key).cloned();
        let result = if *self.fail_pause.lock().unwrap() {
            Err("runsc pause failed: injected".to_string())
        } else {
            match record {
                Some(record) if record["state"] == "running" && !self.is_paused(&key.0, key.1) && check(&record) => {
                    self.pauses.lock().unwrap().push(key.clone());
                    self.mark(key);
                    self.stats.add(&[(Counter::Pauses, 1.0)]);
                    Ok(true)
                }
                _ => Ok(false),
            }
        };
        Box::pin(async move { result })
    }
}

struct FakeRegistry {
    records: Mutex<Vec<Candidate>>,
    revision: AtomicI64,
    open: Mutex<bool>,
    reads: AtomicUsize,
}

impl Inventory for FakeRegistry {
    fn revision(&self) -> Result<i64, String> {
        Ok(self.revision.load(Ordering::SeqCst))
    }

    fn owned(&self) -> Result<Vec<Candidate>, String> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.records.lock().unwrap().clone())
    }

    fn owned_one(&self, sandbox_id: &str) -> Result<Option<Candidate>, String> {
        Ok(self.records.lock().unwrap().iter().find(|record| record.sandbox.sandbox_id == sandbox_id).cloned())
    }

    fn admission_open(&self) -> Result<bool, String> {
        Ok(*self.open.lock().unwrap())
    }
}

type Outcome = Arc<dyn Fn(&Sandbox) -> Result<Escalated, String> + Send + Sync>;

/// The agent's durable park: a parked result removes the marker (a capture
/// thaws first).
struct FakeAgent {
    tier: Arc<FakeTier>,
    parks: Mutex<Vec<Key>>,
    outcome: Mutex<Option<Outcome>>,
}

impl Escalator for FakeAgent {
    fn escalate(&self, sandbox: &Sandbox) -> BoxFuture<Result<Escalated, String>> {
        let key = (sandbox.sandbox_id.clone(), sandbox.generation);
        self.parks.lock().unwrap().push(key.clone());
        let outcome = self.outcome.lock().unwrap().clone();
        let result = match outcome {
            Some(outcome) => outcome(sandbox),
            None => {
                self.tier.paused.lock().unwrap().remove(&key);
                Ok(Escalated::Parked)
            }
        };
        Box::pin(async move { result })
    }
}

struct FakeDemand(Mutex<MemoryDemand>);

impl DemandSource for FakeDemand {
    fn demand(&self) -> MemoryDemand {
        *self.0.lock().unwrap()
    }
}

/// A fake /proc for the real pressure sampler.
fn write_pressure(proc: &Path, fraction: f64, available: u64, swap: Option<(u64, u64)>) {
    std::fs::create_dir_all(proc.join("pressure")).unwrap();
    let total = (available as f64 / fraction) as u64;
    let mut meminfo = format!("MemTotal: {} kB\nMemAvailable: {} kB\n", total / 1024, available / 1024);
    if let Some((total, free)) = swap {
        meminfo += &format!("SwapTotal: {} kB\nSwapFree: {} kB\n", total / 1024, free / 1024);
    }
    std::fs::write(proc.join("meminfo"), meminfo).unwrap();
    std::fs::write(proc.join("pressure/memory"), "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n").unwrap();
    std::fs::write(proc.join("pressure/io"), "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n").unwrap();
}

fn candidate(id: &str, managed: bool) -> Candidate {
    Candidate {
        sandbox: Sandbox {
            sandbox_id: id.into(),
            generation: 1,
            container_id: format!("c-{id}"),
            bundle: PathBuf::from("/nonexistent"),
            memory_directory: format!("m-{id}"),
            spec_sha256: "a".repeat(64),
        },
        parkable: true,
        managed,
    }
}

fn key(id: &str) -> Key {
    (id.to_string(), 1)
}

type Reclaims = Arc<Mutex<Vec<(Key, u64)>>>;

struct Fixture {
    dir: TempDir,
    clock: Arc<FakeClock>,
    tier: Arc<FakeTier>,
    registry: Arc<FakeRegistry>,
    agent: Arc<FakeAgent>,
    demand: Arc<FakeDemand>,
    reclaims: Reclaims,
    /// What the fake paused reclaim returns, in order (the last repeats).
    results: Arc<Mutex<Vec<Result<ReclaimResult, String>>>>,
    engine: Arc<Engine>,
}

fn superseded() -> Result<ReclaimResult, String> {
    Ok(ReclaimResult { requested_bytes: GIB, reclaimed_bytes: GIB / 2, elapsed_seconds: 0.25, refault_file_pages: 0, reason: "superseded" })
}

impl Fixture {
    /// One owned, parkable sandbox "agent" (tests/test_pause_tier.py `_runtime`).
    fn new() -> Fixture {
        let dir = TempDir::new("policy");
        let clock = FakeClock::at(100.0);
        let tier = Arc::new(FakeTier::default());
        let registry = Arc::new(FakeRegistry { records: Mutex::new(vec![candidate("agent", false)]), revision: AtomicI64::new(1), open: Mutex::new(true), reads: AtomicUsize::new(0) });
        let agent = Arc::new(FakeAgent { tier: tier.clone(), parks: Mutex::new(Vec::new()), outcome: Mutex::new(None) });
        let demand = Arc::new(FakeDemand(Mutex::new(MemoryDemand::default())));
        write_pressure(&dir.0.join("proc"), 0.9, 90 * GIB, None);
        let mut fixture = Fixture {
            engine: Arc::new(Engine::new(test_config(&dir.0), Deps {
                mechanism: tier.clone(),
                fence: PauseFence::new(dir.0.join("locks")),
                inventory: registry.clone(),
                escalator: agent.clone(),
                clock: clock.clone(),
                pressure: Arc::new(PressureSampler::new(dir.0.join("proc"), None, clock.clone())),
                demand: demand.clone(),
                budget: budget(&clock),
                paused_reclaim: None,
            })),
            dir,
            clock,
            tier,
            registry,
            agent,
            demand,
            reclaims: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(vec![superseded()])),
        };
        fixture.engine = fixture.restart();
        fixture
    }

    /// A new engine over the same fakes: a daemon restart.
    fn restart(&self) -> Arc<Engine> {
        let (reclaims, results) = (self.reclaims.clone(), self.results.clone());
        let reclaim: PausedReclaim = Arc::new(move |key, target| {
            reclaims.lock().unwrap().push((key.clone(), target));
            let mut results = results.lock().unwrap();
            if results.len() > 1 { results.remove(0) } else { results[0].clone() }
        });
        Arc::new(Engine::new(test_config(&self.dir.0), Deps {
            mechanism: self.tier.clone(),
            fence: PauseFence::new(self.dir.0.join("locks")),
            inventory: self.registry.clone(),
            escalator: self.agent.clone(),
            clock: self.clock.clone(),
            pressure: Arc::new(PressureSampler::new(self.dir.0.join("proc"), None, self.clock.clone())),
            demand: self.demand.clone(),
            budget: budget(&self.clock),
            paused_reclaim: Some(reclaim),
        }))
    }

    /// Fake /proc/meminfo; the sampler's 100 ms cache expires.
    fn pressure(&self, fraction: f64, available: u64, swap: Option<(u64, u64)>) {
        write_pressure(&self.dir.0.join("proc"), fraction, available, swap);
        self.clock.advance(0.2);
    }

    /// Every paused wait measured at 1 GiB resident (the sampler's cache).
    fn sample_all(&self, engine: &Engine) {
        for key in self.tier.markers() {
            engine.sampler_insert(&key, GIB, 0);
        }
    }

    /// One tick, then its jobs run to completion (Python's `_drain`).
    fn tick(&self, engine: &Engine) -> (usize, usize) {
        self.sample_all(engine);
        let jobs = engine.tick();
        let counts = jobs.iter().fold((0, 0), |(reclaims, escalations), job| match job {
            Job::Reclaim(..) => (reclaims + 1, escalations),
            Job::Escalate(..) => (reclaims, escalations + 1),
        });
        run(engine, jobs);
        counts
    }

    fn journal(&self, id: &str) {
        let record = json!({"state": "running", "authority": "live", "sentry_pid": 4242, "sentry_start_time_ticks": 777});
        self.tier.journals.lock().unwrap().insert(key(id), record.as_object().unwrap().clone());
    }

    fn stat(&self, counter: Counter) -> i64 {
        self.tier.stats.get(counter)
    }
}

/// An unthrottled budget whose waits advance the fake clock.
fn budget(clock: &Arc<FakeClock>) -> ReclaimBudget {
    let advance = clock.clone();
    ReclaimBudget::new(RECLAIM_CONCURRENCY, 1e12, clock.clone(), Arc::new(move |seconds| advance.advance(seconds)), FakeNice::new())
}

fn test_config(root: &Path) -> PolicyConfig {
    // Containment checks compare canonical paths.
    let root = std::fs::canonicalize(root).unwrap();
    PolicyConfig {
        session: "session-test".into(),
        state_root: root.join("state"),
        idle_park_seconds: 1.0,
        proc_root: root.join("proc"),
        cgroup_root: root.join("cgroup"),
        memory_backing_root: None,
        tick: Duration::from_millis(10),
    }
}

fn run(engine: &Engine, jobs: Vec<Job>) {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    for job in jobs {
        match job {
            Job::Reclaim(key, target) => engine.run_reclaim(&key, target),
            Job::Escalate(key, sandbox) => runtime.block_on(engine.run_escalation(key, sandbox)),
        }
    }
}

impl Engine {
    /// Seed the sampler's cache as the 1 Hz pass would.
    fn sampler_insert(&self, key: &Key, current: u64, swap: u64) {
        self.sampler.insert_for_tests(key, current, swap, self.clock.monotonic());
    }
}

#[test]
fn drain_demand_alone_never_swaps_out_paused_sandboxes() {
    let fixture = Fixture::new();
    fixture.tier.mark(key("agent"));
    // Closed admission bills nothing (`AgentDemandSource`), so the same huge
    // demand a closed ledger reports is never a reason to reclaim.
    *fixture.demand.0.lock().unwrap() = MemoryDemand::default();
    assert_eq!(fixture.tick(&fixture.engine), (0, 0));
    // The same demand from open admission is real,
    *fixture.demand.0.lock().unwrap() = MemoryDemand { physical_bytes: 1 << 63, ram_backing_bytes: 1 << 63 };
    fixture.tier.mark(key("agent2"));
    fixture.tier.mark(key("agent3"));
    assert_eq!(fixture.tick(&fixture.engine), (0, 0)); // but only forecasts: a burst gets 1 s to settle,
    fixture.engine.rewind_forecast(1.0);
    assert_eq!(fixture.tick(&fixture.engine), (1, 0)); // then waits are released one at a time.
    assert_eq!(fixture.reclaims.lock().unwrap().len(), 1);
    assert_eq!(fixture.engine.status().reason, "queued_demand");
}

#[test]
fn closed_admission_from_the_drain_row_or_the_agent_bills_nothing() {
    let fixture = Fixture::new();
    let directory = fixture.dir.0.join("state/noded");
    crate::fsutil::ensure_private_dir(&directory).unwrap();
    let registry = fixture.registry.clone();
    let source = files::AgentDemandSource {
        file: files::DemandFile::new(&fixture.dir.0.join("state"), Arc::new(SystemClock::new())),
        admission_open: Box::new(move || registry.admission_open().unwrap()),
    };
    std::fs::write(directory.join(files::DEMAND_FILE), r#"{"seq": 1, "admission_open": true, "physical_bytes": 5, "ram_backing_bytes": 6}"#).unwrap();
    std::fs::set_permissions(directory.join(files::DEMAND_FILE), std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(source.demand(), MemoryDemand { physical_bytes: 5, ram_backing_bytes: 6 });
    *fixture.registry.open.lock().unwrap() = false; // The drain row closed it.
    assert_eq!(source.demand(), MemoryDemand::default());
}

#[test]
fn reclaim_runs_only_under_pressure_and_records_metrics() {
    let fixture = Fixture::new();
    // A marker that survived a restart is adopted: managed (relay) waits
    // never reach the idle loop.
    fixture.tier.mark(key("agent"));
    fixture.engine.record_pause(key("gone"), None, ""); // Thawed elsewhere.
    assert_eq!(fixture.tick(&fixture.engine), (0, 0));
    assert!(fixture.reclaims.lock().unwrap().is_empty());
    let keys: Vec<Key> = fixture.engine.paused_waits().into_iter().map(|(key, _)| key).collect();
    assert_eq!(keys, [key("agent")]);
    assert_eq!(fixture.engine.status().reason, "resident_headroom");

    fixture.pressure(0.01, GIB / 8, None);
    assert_eq!(fixture.tick(&fixture.engine), (1, 0));
    assert_eq!(*fixture.reclaims.lock().unwrap(), [(key("agent"), GIB)]);
    assert_eq!(fixture.engine.paused_wait(&key("agent")).unwrap().reclaiming, 0);
    let status = fixture.engine.write_status().unwrap();
    let stats = &status["pause_stats"];
    assert_eq!((&stats["pause_reclaims"], &stats["pause_reclaimed_bytes"], &stats["pause_reclaim_cancellations"]), (&json!(1), &json!(GIB / 2), &json!(1)));
    assert_eq!((&stats["pause_reclaim_ms_total"], &stats["pause_reclaim_stalls"], &status["paused_sandboxes"]), (&json!(250), &json!(0), &json!(1)));
    assert_eq!(status["resident_wait_extra"], json!({"reason": "memory_headroom", "reclaim_target_bytes": 1879048192u64}));
    // The file says the same, under <state_root>/noded.
    let written: Value = serde_json::from_slice(&std::fs::read(fixture.dir.0.join("state/noded/status.json")).unwrap()).unwrap();
    assert_eq!(written, status);
}

#[test]
fn reclaims_in_flight_never_exceed_the_node_budget() {
    let fixture = Fixture::new();
    fixture.pressure(0.01, GIB, None); // 6.5 GiB short.
    for index in 0..5 {
        fixture.tier.mark(key(&format!("agent{index}")));
    }
    let mut started = Vec::new();
    for _ in 0..3 {
        fixture.sample_all(&fixture.engine);
        started.extend(fixture.engine.tick()); // Nothing finishes meanwhile.
    }
    assert_eq!(started.len(), RECLAIM_CONCURRENCY);
    let reclaiming = fixture.engine.paused_waits().iter().filter(|(_, wait)| wait.reclaiming != 0).count();
    assert_eq!(reclaiming, RECLAIM_CONCURRENCY);
    run(&fixture.engine, started);
    assert_eq!(fixture.reclaims.lock().unwrap().len(), RECLAIM_CONCURRENCY);
    assert!(fixture.engine.paused_waits().iter().all(|(_, wait)| wait.reclaiming == 0));
}

#[test]
fn swap_nearly_full_escalates_the_best_waits_through_the_durable_park() {
    let fixture = Fixture::new();
    fixture.pressure(0.01, GIB, Some((16 * GIB, GIB)));
    for index in 0..3 {
        fixture.registry.records.lock().unwrap().push(candidate(&format!("agent{index}"), true));
        fixture.tier.mark(key(&format!("agent{index}")));
    }
    assert_eq!(fixture.tick(&fixture.engine), (0, ESCALATION_CONCURRENCY));
    assert!(fixture.reclaims.lock().unwrap().is_empty()); // Swap is the kernel's reserve now.
    assert_eq!(fixture.agent.parks.lock().unwrap().len(), ESCALATION_CONCURRENCY);
    assert_eq!(fixture.tier.markers().len(), 3 - ESCALATION_CONCURRENCY);
    assert!(fixture.engine.paused_waits().iter().all(|(_, wait)| !wait.escalating));
    assert_eq!(fixture.stat(Counter::PauseEscalations), ESCALATION_CONCURRENCY as i64);
    // In flight, the slots are taken: a tick starts no third.
    fixture.tier.mark(key("agent0"));
    fixture.tier.mark(key("agent1"));
    fixture.sample_all(&fixture.engine);
    let jobs = fixture.engine.tick();
    assert_eq!(jobs.len(), ESCALATION_CONCURRENCY);
    fixture.sample_all(&fixture.engine);
    assert!(fixture.engine.tick().is_empty());
    run(&fixture.engine, jobs);
}

#[test]
fn stalled_or_failed_reclaims_back_off_then_escalate_after_max_stalls() {
    let not_shrinking = Ok(ReclaimResult { requested_bytes: GIB, reclaimed_bytes: MIB, elapsed_seconds: 1.0, refault_file_pages: 0, reason: "not_shrinking" });
    for outcome in [not_shrinking, Err("memory.reclaim is unsupported".to_string())] {
        let fixture = Fixture::new();
        fixture.pressure(0.01, GIB, None);
        fixture.tier.mark(key("agent"));
        *fixture.results.lock().unwrap() = vec![outcome.clone()];
        for stall in 1..=MAX_STALLS {
            fixture.tick(&fixture.engine);
            let wait = fixture.engine.paused_wait(&key("agent")).unwrap();
            assert_eq!((wait.stalls, fixture.agent.parks.lock().unwrap().len()), (stall, 0));
            fixture.tick(&fixture.engine); // Backing off: nothing happens.
            assert_eq!(fixture.reclaims.lock().unwrap().len(), stall as usize);
            fixture.engine.with_wait(&key("agent"), |wait| wait.retry_at = 0.0); // The backoff ends.
        }
        fixture.tick(&fixture.engine);
        assert_eq!(*fixture.agent.parks.lock().unwrap(), [key("agent")]);
        assert_eq!((fixture.stat(Counter::PauseReclaimStalls), fixture.stat(Counter::PauseEscalations)), (MAX_STALLS as i64, 1));
        let stopped = if outcome.is_ok() { Counter::PauseReclaimNotShrinking } else { Counter::PauseReclaimErrors };
        assert_eq!(fixture.stat(stopped), MAX_STALLS as i64);
        assert_eq!(fixture.stat(Counter::PauseReclaims), if outcome.is_ok() { MAX_STALLS as i64 } else { 0 });
    }
}

#[test]
fn escalation_loses_to_activity_and_a_refused_capture_stays_paused() {
    let fixture = Fixture::new();
    let sandbox = candidate("agent", false).sandbox;
    fixture.engine.record_pause(key("agent"), None, "");
    fixture.engine.with_wait(&key("agent"), |wait| wait.escalating = true);
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    runtime.block_on(fixture.engine.run_escalation(key("agent"), sandbox.clone())); // Thawed after the plan: no marker.
    assert!(fixture.agent.parks.lock().unwrap().is_empty());
    assert!(!fixture.engine.paused_wait(&key("agent")).unwrap().escalating);

    fixture.tier.mark(key("agent"));
    *fixture.agent.outcome.lock().unwrap() = Some(Arc::new(|_| Err("409 park_deferred: no disk for the capture".into())));
    fixture.engine.with_wait(&key("agent"), |wait| wait.escalating = true);
    runtime.block_on(fixture.engine.run_escalation(key("agent"), sandbox.clone()));
    assert_eq!(fixture.agent.parks.lock().unwrap().len(), 1);
    assert!(!fixture.engine.paused_wait(&key("agent")).unwrap().escalating); // The next tick retries.
    assert!(fixture.tier.markers().contains(&key("agent")));
    // Still running: answered but nothing captured.
    *fixture.agent.outcome.lock().unwrap() = Some(Arc::new(|_| Ok(Escalated::Running)));
    runtime.block_on(fixture.engine.run_escalation(key("agent"), sandbox.clone()));
    assert_eq!(fixture.stat(Counter::PauseEscalations), 0);
    // A local wait whose call was answered is never hibernated.
    *fixture.engine.answered.write().unwrap() = Some(Arc::new(|id, _| id == "agent"));
    runtime.block_on(fixture.engine.run_escalation(key("agent"), sandbox));
    assert_eq!(fixture.agent.parks.lock().unwrap().len(), 2);
}

#[test]
fn a_restart_after_a_crashed_escalation_escalates_again() {
    let fixture = Fixture::new();
    fixture.pressure(0.01, GIB, Some((16 * GIB, 0)));
    fixture.tier.mark(key("agent"));
    for error in ["a bug", "node agent killed"] {
        let message = error.to_string();
        *fixture.agent.outcome.lock().unwrap() = Some(Arc::new(move |_| Err(message.clone())));
        let before = fixture.agent.parks.lock().unwrap().len();
        assert_eq!(fixture.tick(&fixture.engine), (0, 1));
        assert_eq!(fixture.agent.parks.lock().unwrap().len(), before + 1); // Any error returns the slot.
        assert!(!fixture.engine.paused_wait(&key("agent")).unwrap().escalating);
        assert!(fixture.tier.markers().contains(&key("agent"))); // The marker survives.
    }
    *fixture.agent.outcome.lock().unwrap() = None;
    let restarted = fixture.restart();
    assert_eq!(fixture.tick(&restarted), (0, 1));
    assert!(fixture.tier.markers().is_empty());
    // A marker whose registration is gone has nothing to hibernate.
    fixture.tier.mark(key("orphan"));
    assert_eq!(fixture.tick(&restarted), (0, 0));
}

#[test]
fn a_thawing_wait_is_superseded_before_any_window() {
    let fixture = Fixture::new();
    let engine = fixture.restart_with_kernel_reclaim();
    fixture.tier.mark(key("agent"));
    fixture.tier.thawing.lock().unwrap().insert(key("agent"));
    engine.run_reclaim(&key("agent"), GIB);
    assert_eq!((fixture.stat(Counter::PauseReclaimCancellations), fixture.stat(Counter::PauseReclaimStalls)), (1, 0));
    // No longer this generation: an error, counted as a stall.
    fixture.registry.records.lock().unwrap()[0].sandbox.generation = 2;
    engine.run_reclaim(&key("agent"), GIB);
    assert_eq!((fixture.stat(Counter::PauseReclaimStalls), fixture.stat(Counter::PauseReclaimErrors)), (1, 1));
    // Paused but unmeasured: an error too, never a guess.
    fixture.registry.records.lock().unwrap()[0].sandbox.generation = 1;
    fixture.tier.thawing.lock().unwrap().clear();
    fixture.journal("agent");
    engine.run_reclaim(&key("agent"), GIB);
    assert_eq!(fixture.stat(Counter::PauseReclaimErrors), 2);
}

impl Fixture {
    fn restart_with_kernel_reclaim(&self) -> Arc<Engine> {
        Arc::new(Engine::new(test_config(&self.dir.0), Deps {
            mechanism: self.tier.clone(),
            fence: PauseFence::new(self.dir.0.join("locks")),
            inventory: self.registry.clone(),
            escalator: self.agent.clone(),
            clock: self.clock.clone(),
            pressure: Arc::new(PressureSampler::new(self.dir.0.join("proc"), None, self.clock.clone())),
            demand: self.demand.clone(),
            budget: budget(&self.clock),
            paused_reclaim: None,
        }))
    }

    /// /proc/<4242>/stat for the journal's sentry.
    fn sentry(&self, ticks: u64) {
        let process = self.dir.0.join("proc/4242");
        std::fs::create_dir_all(&process).unwrap();
        std::fs::write(process.join("stat"), format!("4242 (runsc-sandbox) S {}{ticks} 0", "0 ".repeat(18))).unwrap();
    }
}

fn wall_now() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64()
}

/// An idle pass, retried while a lock just released is still held by a child
/// another test thread is forking (it holds every descriptor until its exec).
fn settle(runtime: &tokio::runtime::Runtime, engine: &Engine) -> Vec<(Key, IdleOutcome)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let outcomes = runtime.block_on(engine.idle_pass());
        if !outcomes.iter().any(|(_, outcome)| *outcome == IdleOutcome::Busy) || std::time::Instant::now() >= deadline {
            return outcomes;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn idle_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn idle_timer_pauses_and_adopts_existing_pauses() {
    let fixture = Fixture::new();
    fixture.journal("agent");
    fixture.sentry(777);
    let runtime = idle_runtime();
    // The first observation counts as activity.
    assert!(runtime.block_on(fixture.engine.idle_pass()).is_empty());
    fixture.clock.advance(1.0);
    assert_eq!(runtime.block_on(fixture.engine.idle_pass()), [(key("agent"), IdleOutcome::Paused)]);
    assert_eq!(*fixture.tier.pauses.lock().unwrap(), [key("agent")]);
    assert!(fixture.engine.paused_wait(&key("agent")).is_some());
    // Further passes find the marker and do not pause again.
    assert_eq!(runtime.block_on(fixture.engine.idle_pass()), [(key("agent"), IdleOutcome::AlreadyPaused)]);
    assert_eq!(fixture.tier.pauses.lock().unwrap().len(), 1);
    assert_eq!(fixture.stat(Counter::Pauses), 1);
}

#[test]
fn idle_pause_skips_managed_busy_active_and_dead_sandboxes() {
    let fixture = Fixture::new();
    fixture.registry.records.lock().unwrap().push(candidate("managed", true));
    fixture.registry.records.lock().unwrap().push(Candidate { parkable: false, ..candidate("pinned", false) });
    for id in ["agent", "managed", "pinned"] {
        fixture.journal(id);
    }
    fixture.sentry(777);
    fixture.clock.set_wall(wall_now());
    let runtime = idle_runtime();
    runtime.block_on(fixture.engine.idle_pass());
    fixture.clock.advance(5.0);

    // An exec holds A: busy, the next tick decides.
    let execs = crate::exec_fence::ExecFence::new(fixture.dir.0.join("locks"));
    std::fs::create_dir_all(fixture.dir.0.join("locks")).unwrap();
    let crate::exec_fence::Fenced::Held(lease) = execs.acquire("agent").unwrap() else { panic!("fence busy") };
    assert_eq!(runtime.block_on(fixture.engine.idle_pass()), [(key("agent"), IdleOutcome::Busy)]);
    // It touched the activity clock when it ended: active again.
    lease.touch();
    fixture.clock.set_wall(wall_now());
    drop(lease);
    assert!(settle(&runtime, &fixture.engine).is_empty());
    // Idle again by the activity clock.
    fixture.clock.advance(1.5);
    // The sentry died (or a pid was reused): the agent reconciles it.
    fixture.sentry(778);
    assert_eq!(settle(&runtime, &fixture.engine), [(key("agent"), IdleOutcome::NotRunning)]);
    fixture.sentry(777);
    *fixture.tier.fail_pause.lock().unwrap() = true;
    assert_eq!(settle(&runtime, &fixture.engine), [(key("agent"), IdleOutcome::Declined)]);
    *fixture.tier.fail_pause.lock().unwrap() = false;
    assert_eq!(settle(&runtime, &fixture.engine), [(key("agent"), IdleOutcome::Paused)]);
    // Managed and unparkable sandboxes were never candidates.
    assert_eq!(*fixture.tier.pauses.lock().unwrap(), [key("agent")]);
}

#[test]
fn idle_candidates_are_reread_only_when_the_registry_revision_moves() {
    let fixture = Fixture::new();
    let runtime = idle_runtime();
    for _ in 0..3 {
        runtime.block_on(fixture.engine.idle_pass());
    }
    assert_eq!(fixture.registry.reads.load(Ordering::SeqCst), 1);
    fixture.registry.records.lock().unwrap().push(candidate("new", false));
    fixture.registry.revision.fetch_add(1, Ordering::SeqCst);
    fixture.journal("new");
    fixture.journal("agent");
    fixture.sentry(777);
    fixture.clock.advance(1.0);
    let outcomes = runtime.block_on(fixture.engine.idle_pass());
    assert_eq!(fixture.registry.reads.load(Ordering::SeqCst), 2);
    // The new candidate's first observation is now: only "agent" is idle.
    assert_eq!(outcomes, [(key("agent"), IdleOutcome::Paused)]);
    // A generation that changed under the fence is not paused.
    fixture.registry.records.lock().unwrap()[1].sandbox.generation = 2;
    fixture.clock.advance(1.0);
    assert_eq!(runtime.block_on(fixture.engine.idle_pass()), [(key("agent"), IdleOutcome::AlreadyPaused), (key("new"), IdleOutcome::Gone)]);
}

#[test]
fn the_sampler_pass_measures_paused_owned_incarnations_only() {
    let fixture = Fixture::new();
    let container = "c-agent";
    let cgroup = fixture.dir.0.join("cgroup/ucloud-sandboxes").join(container);
    std::fs::create_dir_all(&cgroup).unwrap();
    std::fs::write(cgroup.join("memory.current"), "3000").unwrap();
    std::fs::write(cgroup.join("memory.swap.current"), "500").unwrap();
    std::fs::write(cgroup.join("memory.stat"), "shmem 0\nanon 100\nfile 800\nfile_dirty 0\nfile_writeback 0\nworkingset_refault_file 0\n").unwrap();
    fixture.sentry(777);
    std::fs::write(fixture.dir.0.join("proc/4242/cgroup"), format!("0::/ucloud-sandboxes/{container}\n")).unwrap();
    let bundle = fixture.dir.0.join("bundle");
    std::fs::create_dir_all(&bundle).unwrap();
    std::fs::write(bundle.join("config.json"), format!(r#"{{"linux": {{"cgroupsPath": "/ucloud-sandboxes/{container}"}}}}"#)).unwrap();
    fixture.registry.records.lock().unwrap()[0].sandbox.bundle = bundle;
    fixture.journal("agent");
    let engine = &fixture.engine;
    engine.sample_paused();
    assert!(engine.sampler.get(&key("agent")).is_none()); // Not paused: not sampled here.
    fixture.tier.mark(key("agent"));
    engine.sample_if_due();
    let sample = engine.sampler.get(&key("agent")).unwrap();
    assert_eq!((sample.current_bytes, sample.swap_bytes), (3000, 500));
    // The tick prices waits with these samples (0.2 s old, still live).
    fixture.pressure(0.01, GIB, None);
    let jobs = engine.tick();
    assert!(matches!(&jobs[..], [Job::Reclaim(key, 3000)] if key.0 == "agent"), "{jobs:?}");
    assert_eq!(engine.paused_wait(&key("agent")).unwrap().swapped_bytes, 500);
    // Thawed: the next pass forgets it.
    fixture.tier.paused.lock().unwrap().clear();
    fixture.clock.advance(1.0);
    engine.sample_if_due();
    assert!(engine.sampler.get(&key("agent")).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_policy_runs_until_stopped_and_publishes_its_status() {
    let fixture = Fixture::new();
    fixture.tier.mark(key("agent"));
    let handle = PausePolicy::start_engine(fixture.restart());
    let status = fixture.dir.0.join("state/noded/status.json");
    let mut written = None;
    for _ in 0..500 {
        if let Ok(bytes) = std::fs::read(&status)
            && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
        {
            written = Some(value);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let written = written.expect("status.json was written");
    assert_eq!(written["paused_sandboxes"], 1);
    assert_eq!(handle.engine().paused_waits().len(), 1);
    assert_eq!(handle.status().reason, "resident_headroom");
    handle.stop().await;
    let seq = serde_json::from_slice::<Value>(&std::fs::read(&status).unwrap()).unwrap()["seq"].clone();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(serde_json::from_slice::<Value>(&std::fs::read(&status).unwrap()).unwrap()["seq"], seq); // Stopped.
}

/// The real pause tier under the policy, with a fake runsc: an idle pause
/// writes the marker, runs `runsc pause`, and the tick adopts it.
#[test]
fn the_real_pause_tier_pauses_an_idle_sandbox() {
    let dir = TempDir::new("policy-tier");
    let root = dir.0.clone();
    for sub in ["bin", "fake", "runtime", "journals", "bundles/b-1", "proc/4242", "cgroup", "state"] {
        std::os::unix::fs::DirBuilderExt::mode(std::fs::DirBuilder::new().recursive(true), 0o700).create(root.join(sub)).unwrap();
    }
    let runsc = root.join("bin/runsc");
    let script = format!(
        "#!/bin/sh\nfor arg in \"$@\"; do case \"$arg\" in --*) ;; *) verb=\"$arg\"; break ;; esac; done\n\
         echo \"$verb\" >> '{fake}/commands'\n\
         [ \"$verb\" = state ] && printf '{{\"id\":\"c-1\",\"pid\":4242,\"status\":\"running\"}}\\n'\nexit 0\n",
        fake = root.join("fake").display()
    );
    std::fs::write(&runsc, script).unwrap();
    std::fs::set_permissions(&runsc, std::fs::Permissions::from_mode(0o755)).unwrap();
    warm_up(&runsc);
    let _ = std::fs::remove_file(root.join("fake/commands"));
    std::fs::write(root.join("proc/4242/stat"), format!("4242 (runsc-sandbox) S {}777 0", "0 ".repeat(18))).unwrap();
    std::fs::write(root.join("zswap"), "N\n").unwrap();
    crate::journal::JournalStore::new(root.join("journals")).initialize_running("sb-1", 1, &"a".repeat(64), "create-1", 4242, 777).unwrap();
    let warden = crate::warden::WardenConfig {
        runsc: runsc.clone(),
        runtime_root: root.join("runtime"),
        bundle_root: root.join("bundles"),
        journal_root: root.join("journals"),
        memory_root: root.join("memory"),
        application_memory_root: None,
        network: "none".into(),
        reflink_memory_restore: false,
        proc_root: root.join("proc"),
        command_timeout: Duration::from_secs(10),
        stop_timeout: Duration::from_secs(1),
    };
    let mut pause = crate::pause::PauseConfig::new(warden, true);
    pause.cgroup_root = root.join("cgroup");
    pause.zswap_enabled = root.join("zswap");
    let tier = crate::pause::PauseTier::new(pause.clone(), None);
    let sandbox = Sandbox {
        sandbox_id: "sb-1".into(),
        generation: 1,
        container_id: "c-1".into(),
        bundle: root.join("bundles/b-1"),
        memory_directory: "m-1".into(),
        spec_sha256: "a".repeat(64),
    };
    let registry = Arc::new(FakeRegistry {
        records: Mutex::new(vec![Candidate { sandbox: sandbox.clone(), parkable: true, managed: false }]),
        revision: AtomicI64::new(1),
        open: Mutex::new(true),
        reads: AtomicUsize::new(0),
    });
    let clock = FakeClock::at(10.0);
    write_pressure(&root.join("proc"), 0.9, 90 * GIB, None);
    let mut config = PolicyConfig::new(root.join("state"), "session-test".into(), 1.0, &pause);
    config.tick = Duration::from_millis(10);
    let engine = Engine::new(config, Deps {
        mechanism: Arc::new(tier.clone()),
        fence: PauseFence::new(root.join("runtime/warden-locks")),
        inventory: registry,
        escalator: Arc::new(FakeAgent { tier: Arc::new(FakeTier::default()), parks: Mutex::new(Vec::new()), outcome: Mutex::new(None) }),
        clock: clock.clone(),
        pressure: Arc::new(PressureSampler::new(root.join("proc"), None, clock.clone())),
        demand: Arc::new(FakeDemand(Mutex::new(MemoryDemand::default()))),
        budget: budget(&clock),
        paused_reclaim: None,
    });
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    runtime.block_on(engine.idle_pass());
    clock.advance(2.0);
    assert_eq!(runtime.block_on(engine.idle_pass()), [(("sb-1".to_string(), 1), IdleOutcome::Paused)]);
    assert!(tier.is_paused("sb-1", 1));
    assert_eq!(std::fs::read_to_string(root.join("fake/commands")).unwrap().lines().collect::<Vec<_>>(), ["pause"]);
    assert_eq!(tier.stats().get(Counter::Pauses), 1);
    engine.tick();
    assert_eq!(engine.status().paused_sandboxes, 1);
    // Paused: the next pass leaves it.
    assert_eq!(runtime.block_on(engine.idle_pass()), [(("sb-1".to_string(), 1), IdleOutcome::AlreadyPaused)]);
}

/// Exec a freshly written script until no forked sibling still holds it open
/// for writing (ETXTBSY).
fn warm_up(script: &Path) {
    for _ in 0..200 {
        match std::process::Command::new(script).arg("--root=/nonexistent").arg("warm-up").output() {
            Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => std::thread::sleep(Duration::from_millis(5)),
            result => {
                result.unwrap();
                return;
            }
        }
    }
    panic!("the fake runsc stayed busy");
}
