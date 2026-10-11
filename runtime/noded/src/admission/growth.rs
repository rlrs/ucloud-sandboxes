//! Managed primary-process growth (ucloud_sandboxes/direct_service.py
//! `_growth_sample` .. `_update_growth_forecast_locked`, `_growth_turn`):
//! the cost functions, the in-memory intents and provisional claims, credit
//! expiry, and per-key turns.
//!
//! The durable `managed_growth` rows stay the registry's
//! ([`GrowthStore`], implemented by [`Registry`]); this module holds what the
//! agent keeps in memory beside them. Callers own the ordering: take the key's
//! [`Turns`] turn, commit through the store, then apply the result here under
//! the admission lock ([`Growth::record`]). Nothing here is wired yet
//! (docs/admission-port-3c.md, step 3c.1).

use std::collections::HashMap;
use std::sync::Arc;

use super::Owner;
use super::ledger::{Cost, Kind, Ledger};
use crate::pause::prefetch::PREFETCH_MAX_BYTES;
use crate::pause_policy::resident::ResidentSample;
use crate::registry::{self, GrowthAction, GrowthIntent, Registry, RegistryError};

/// Growth credit may use an observation this old; see [`growth_sample`].
pub const GROWTH_OBSERVATION_MAX_AGE_SECONDS: f64 = 30.0;

const MIB: u64 = 1024 * 1024;

/// `warden.application_memory_mode`: where the guest's application memory lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryMode {
    Ram,
    File,
}

impl MemoryMode {
    pub fn parse(text: &str) -> Option<MemoryMode> {
        match text {
            "ram" => Some(MemoryMode::Ram),
            "file" => Some(MemoryMode::File),
            _ => None,
        }
    }
}

/// `_growth_sample`: an observation counts for 30 s. Park and restore forget
/// samples, so any sample is this runtime's. Growth since the sample is
/// double-charged (conservative); release is bounded by the age fence.
pub fn growth_sample(sample: Option<&ResidentSample>, now: f64) -> Option<&ResidentSample> {
    sample.filter(|sample| !(now - sample.sampled_at > GROWTH_OBSERVATION_MAX_AGE_SECONDS))
}

/// `_growth_remaining`: the bound less observed resident pages. On RAM
/// backing only shmem counts, the charge shared by physical memory and
/// tmpfs; page cache cannot buy future tmpfs space.
pub fn growth_remaining(mode: MemoryMode, bound: u64, sample: Option<&ResidentSample>, now: f64) -> u64 {
    let observed = match growth_sample(sample, now) {
        None => 0,
        Some(sample) if mode == MemoryMode::Ram => sample.current_bytes.min(sample.shared_memory_bytes),
        Some(sample) => sample.current_bytes,
    };
    bound.saturating_sub(observed)
}

/// `_continuation_growth`: (physical, RAM-backing) growth a continuation
/// needs to return to its demonstrated peak. A swapped continuation owes
/// physical headroom for its thaw's prefetch, not its whole swap, and its
/// swapped tmpfs blocks are still allocated.
pub fn continuation_growth(mode: MemoryMode, bound: u64, sample: Option<&ResidentSample>, now: f64) -> (u64, u64) {
    let sample = match growth_sample(sample, now) {
        Some(sample) if sample.peak_bytes != 0 => sample,
        sample => {
            let remaining = growth_remaining(mode, bound, sample, now);
            return (remaining, remaining);
        }
    };
    let ceiling = bound.min(sample.peak_bytes);
    let shared = sample.current_bytes.min(sample.shared_memory_bytes);
    let swapped = sample.swap_bytes.min(ceiling.saturating_sub(sample.current_bytes));
    let physical = ceiling.saturating_sub(sample.current_bytes).saturating_sub(swapped) + swapped.min(PREFETCH_MAX_BYTES);
    (physical, ceiling.saturating_sub(shared).saturating_sub(swapped))
}

/// `_transition_memory_cost`: RAM backing follows memory unless given, and
/// is 0 off RAM placement.
pub fn transition_memory_cost(
    kind: Kind,
    mode: MemoryMode,
    memory_bytes: Option<u64>,
    ram_backing_bytes: Option<u64>,
    read_bytes: Option<u64>,
    provenance: &str,
) -> Cost {
    let ram_backing_bytes = match mode {
        MemoryMode::Ram => ram_backing_bytes.or(memory_bytes),
        MemoryMode::File => Some(0),
    };
    Cost { kind, memory_bytes, read_bytes, write_bytes: None, provenance: provenance.into(), ram_backing_bytes }
}

/// `_growth_cost`: launches keep the whole bound until their first safe
/// wait; continuations go back to their peak; file-backed continuations are
/// reclaimable, so unknown rather than a fictional zero.
pub fn growth_cost(mode: MemoryMode, bound: u64, kind: Kind, request_id: &str, sample: Option<&ResidentSample>, now: f64) -> Cost {
    if mode == MemoryMode::File && !request_id.is_empty() {
        return transition_memory_cost(kind, mode, None, None, None, "reclaimable-file-growth");
    }
    if request_id.is_empty() {
        let remaining = growth_remaining(mode, bound, sample, now);
        return transition_memory_cost(kind, mode, Some(remaining), None, None, "primary-process-growth-forecast");
    }
    let (physical, ram_backing) = continuation_growth(mode, bound, sample, now);
    transition_memory_cost(kind, mode, Some(physical), Some(ram_backing), None, "primary-process-growth-forecast")
}

/// `_startup_cost`: a managed guest runs nothing until its launch, which
/// carries the bound, so its create costs 0.
pub fn startup_cost(mode: MemoryMode, requested_memory_mb: u64, managed: bool) -> Cost {
    if managed {
        return transition_memory_cost(Kind::Startup, mode, Some(0), None, None, "managed-guest-before-launch");
    }
    transition_memory_cost(Kind::Startup, mode, Some(requested_memory_mb * MIB), None, None, "requested-memory-bound")
}

/// What forecasts read about each owner: the last resident sample (aged by
/// the caller) and the memory placement.
pub trait Observations {
    fn historical(&self, key: &Owner) -> Option<ResidentSample>;
    fn memory_mode(&self, key: &Owner) -> MemoryMode;
}

pub fn intent_key(intent: &GrowthIntent) -> Owner {
    (intent.sandbox_id.clone(), intent.generation as u64)
}

/// The in-memory half of managed growth. Every method runs under the
/// admission lock and updates the ledger's forecast for the keys it changes.
#[derive(Debug)]
pub struct Growth {
    intents: HashMap<Owner, GrowthIntent>,
    /// Admitted growth whose durable commit is in flight, charged like an
    /// active intent until it settles.
    provisional: HashMap<Owner, Cost>,
    /// When the oldest sample a cached forecast credits ages out.
    credit_expires: f64,
}

impl Growth {
    /// From the durable rows (`growth_intents()` at start).
    pub fn new(intents: impl IntoIterator<Item = GrowthIntent>) -> Growth {
        Growth {
            intents: intents.into_iter().map(|intent| (intent_key(&intent), intent)).collect(),
            provisional: HashMap::new(),
            credit_expires: f64::NEG_INFINITY,
        }
    }

    pub fn intent(&self, key: &Owner) -> Option<&GrowthIntent> {
        self.intents.get(key)
    }

    pub fn provisional(&self, key: &Owner) -> Option<&Cost> {
        self.provisional.get(key)
    }

    pub fn credit_expires(&self) -> f64 {
        self.credit_expires
    }

    /// `_update_growth_forecast_locked`: one owner's forecast after its
    /// intent changed. An active intent is forecast from its sample; any
    /// other phase charges only a provisional claim.
    pub fn update_forecast(&mut self, key: &Owner, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        let mut cost = self.provisional.get(key).cloned();
        if let Some(item) = self.intents.get(key).filter(|item| item.phase == "active") {
            let historical = observations.historical(key);
            let sample = growth_sample(historical.as_ref(), now);
            let bound = item.memory_bytes.max(0) as u64;
            cost = Some(growth_cost(observations.memory_mode(key), bound, Kind::Restore, &item.request_id, sample, now));
            if let Some(sample) = sample {
                self.credit_expires = self.credit_expires.min(sample.sampled_at + GROWTH_OBSERVATION_MAX_AGE_SECONDS);
            }
        }
        ledger.set_growth_forecast(key, cost);
    }

    /// `_refresh_growth_forecasts_locked`: every forecast, once per sampler
    /// pass rather than per admission.
    pub fn refresh_forecasts(&mut self, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        self.credit_expires = f64::INFINITY;
        ledger.set_growth_forecasts([]);
        let mut keys: Vec<Owner> = self.intents.keys().chain(self.provisional.keys()).cloned().collect();
        keys.sort();
        keys.dedup();
        for key in keys {
            self.update_forecast(&key, ledger, observations, now);
        }
    }

    /// `_current_growth_forecasts_locked`: refresh only once credit ages out.
    pub fn current_forecasts(&mut self, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        if now > self.credit_expires {
            self.refresh_forecasts(ledger, observations, now);
        }
    }

    /// Publish a committed transition (`_record_growth_intent` after the
    /// store answered).
    pub fn record(&mut self, key: &Owner, intent: Option<GrowthIntent>, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        if let Some(intent) = intent {
            self.intents.insert(key.clone(), intent);
        }
        self.update_forecast(key, ledger, observations, now);
    }

    /// `_forget_growth`: delete retires the key's intent and claim.
    pub fn forget(&mut self, key: &Owner, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        self.intents.remove(key);
        self.provisional.remove(key);
        self.update_forecast(key, ledger, observations, now);
    }

    /// Charge admitted bytes before releasing the admission lock, so a
    /// concurrent admission cannot spend them while the commit fsyncs.
    pub fn charge_provisional(&mut self, key: &Owner, cost: Cost, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        self.provisional.insert(key.clone(), cost);
        self.update_forecast(key, ledger, observations, now);
    }

    pub fn clear_provisional(&mut self, key: &Owner, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        self.provisional.remove(key);
        self.update_forecast(key, ledger, observations, now);
    }

    /// Drop intents whose owner no longer holds a registration (as
    /// `refresh_resident_memory` prunes stale rows).
    pub fn retain(&mut self, keep: impl Fn(&Owner) -> bool, ledger: &mut Ledger, observations: &dyn Observations, now: f64) {
        let stale: Vec<Owner> = self.intents.keys().filter(|key| !keep(key)).cloned().collect();
        for key in stale {
            self.intents.remove(&key);
            self.update_forecast(&key, ledger, observations, now);
        }
    }

    /// `_admit_managed_growth`'s covered check: the existing exposure already
    /// covers this request (an active or terminal primary, or a continuation
    /// whose safe wait belongs to another request).
    pub fn covered(&self, key: &Owner, startup: bool, request_id: &str) -> Option<&GrowthIntent> {
        self.intents.get(key).filter(|previous| {
            previous.phase == "active"
                || previous.phase == "terminal"
                || (!startup && previous.phase == "safe" && !previous.request_id.is_empty() && previous.request_id != request_id)
        })
    }
}

/// `_growth_turn`: one sandbox's durable growth writes and in-memory
/// forecast change in the same order, without the node-wide lock. Take a
/// turn before, never under, the admission lock.
#[derive(Debug, Default)]
pub struct Turns {
    turns: std::sync::Mutex<HashMap<Owner, (Arc<tokio::sync::Mutex<()>>, usize)>>,
}

/// A held turn; dropping it lets the key's next writer in.
#[derive(Debug)]
pub struct Turn<'a> {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _registration: Registration<'a>,
}

/// One interest in a key's turn, counted until dropped, so a cancelled wait
/// cannot leak the entry.
#[derive(Debug)]
struct Registration<'a> {
    turns: &'a Turns,
    key: Owner,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut turns = self.turns.lock();
        if let Some(entry) = turns.get_mut(&self.key) {
            entry.1 -= 1;
            if entry.1 == 0 {
                turns.remove(&self.key);
            }
        }
    }
}

impl Turns {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Owner, (Arc<tokio::sync::Mutex<()>>, usize)>> {
        self.turns.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub async fn take(&self, key: &Owner) -> Turn<'_> {
        let mutex = {
            let mut turns = self.lock();
            let entry = turns.entry(key.clone()).or_insert_with(|| (Arc::new(tokio::sync::Mutex::new(())), 0));
            entry.1 += 1;
            entry.0.clone()
        };
        let registration = Registration { turns: self, key: key.clone() };
        Turn { _guard: mutex.lock_owned().await, _registration: registration }
    }

    /// Turns for a batch, in key order (`record_local_wait_growth`).
    pub async fn take_all<'a>(&'a self, keys: impl IntoIterator<Item = &'a Owner>) -> Vec<Turn<'a>> {
        let mut keys: Vec<&Owner> = keys.into_iter().collect();
        keys.sort();
        keys.dedup();
        let mut held = Vec::with_capacity(keys.len());
        for key in keys {
            held.push(self.take(key).await);
        }
        held
    }

    /// Keys with a holder or a waiter.
    pub fn active(&self) -> usize {
        self.lock().len()
    }
}

/// The durable `managed_growth` writer: `Registry::growth_intent*`. Kept a
/// trait so the admission service can be tested without SQLite.
pub trait GrowthStore: Send + Sync {
    fn growth_intent(
        &self,
        key: &Owner,
        action: GrowthAction,
        job_id: &str,
        launch_sha256: &str,
        request_id: &str,
    ) -> registry::Result<Option<GrowthIntent>>;

    /// `(key, action, request ID)` steps as one commit; each item fails alone.
    fn growth_intent_batch(
        &self,
        operations: &[(Owner, GrowthAction, String)],
    ) -> registry::Result<Vec<registry::Result<Option<GrowthIntent>>>>;
}

fn generation(key: &Owner) -> registry::Result<i64> {
    i64::try_from(key.1).map_err(|_| RegistryError::invalid("growth intent generation is out of range"))
}

impl GrowthStore for Registry {
    fn growth_intent(
        &self,
        key: &Owner,
        action: GrowthAction,
        job_id: &str,
        launch_sha256: &str,
        request_id: &str,
    ) -> registry::Result<Option<GrowthIntent>> {
        Registry::growth_intent(self, &key.0, generation(key)?, action, job_id, launch_sha256, request_id)
    }

    fn growth_intent_batch(
        &self,
        operations: &[(Owner, GrowthAction, String)],
    ) -> registry::Result<Vec<registry::Result<Option<GrowthIntent>>>> {
        let operations = operations
            .iter()
            .map(|(key, action, request_id)| Ok(((key.0.clone(), generation(key)?), *action, request_id.clone())))
            .collect::<registry::Result<Vec<_>>>()?;
        Registry::growth_intent_batch(self, &operations)
    }
}
