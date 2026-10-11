//! What the admission service reads in the daemon: the registry, runtime
//! metrics, resident samples and memory placement, the agent's socket; and
//! what it feeds: the pause policy's demand and local waits' growth events.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::mpsc;

use super::Owner;
use super::decide::Metrics;
use super::growth::{MemoryMode, Observations};
use super::service::{AdmissionService, AgentProbe, BoxFuture, Event, EventAction, MetricsSource, OwnerRecord, Owners};
use crate::local_wait::{GrowthAction as WaitAction, GrowthEvent};
use crate::memory_backing::{ActiveMode, MemoryBackingStore};
use crate::pause_policy::resident::{ResidentSample, ResidentSampler};
use crate::pause_policy::{DemandSource, MemoryBackingCapacity, MemoryDemand, Pressure, PressureSource};
use crate::registry::{Phase, Registration, Registry};

const MIB: u64 = 1024 * 1024;
/// `runtime_metrics.py`'s single-flight cache.
pub const METRICS_CACHE: Duration = Duration::from_millis(200);
/// Local waits' growth events are applied in batches this often, at most
/// this many at once (as they were sent to the agent).
pub const GROWTH_BATCH_INTERVAL: Duration = Duration::from_millis(50);
pub const GROWTH_BATCH_ITEMS: usize = 4096;

fn owner_record(record: &Registration) -> OwnerRecord {
    OwnerRecord {
        generation: record.sandbox_generation as u64,
        owned: record.phase == Phase::Owned,
        managed: record.spec.managed_process(),
        memory_mb: record.spec.memory_mb().unwrap_or(0).max(0) as u64,
    }
}

impl Owners for Registry {
    /// A fresh read: the agent commits creates and deletes in its own
    /// process, and asks for their admission right after.
    fn owner(&self, sandbox_id: &str) -> Result<Option<OwnerRecord>, String> {
        let record = self.get_fresh(sandbox_id).map_err(|error| error.to_string())?;
        Ok(record.as_deref().map(owner_record))
    }

    fn owners(&self, sandbox_ids: &[String]) -> Result<HashMap<String, Option<OwnerRecord>>, String> {
        let snapshot = self.snapshot_fresh().map_err(|error| error.to_string())?;
        Ok(sandbox_ids.iter().map(|id| (id.clone(), snapshot.get(id).map(|record| owner_record(record)))).collect())
    }

    fn keys(&self) -> Result<HashSet<Owner>, String> {
        let snapshot = self.snapshot().map_err(|error| error.to_string())?;
        Ok(snapshot.records.iter().map(|record| (record.sandbox_id().to_string(), record.sandbox_generation as u64)).collect())
    }
}

/// `collect_runtime_metrics`' memory fields from one pressure sample.
pub fn metrics_from_pressure(pressure: &Pressure) -> Metrics {
    Metrics {
        memory_total_mb: pressure.memory_total_bytes / MIB,
        memory_available_mb: pressure.memory_available_bytes / MIB,
        swap_total_mb: pressure.swap_total_bytes.unwrap_or(0) / MIB,
        swap_free_mb: pressure.swap_free_bytes.unwrap_or(0) / MIB,
        memory_psi_full_avg10: pressure.memory_full_stall,
        memory_backing: pressure.memory_backing.clone(),
    }
}

/// The node's metrics, one sample per 200 ms however many admissions ask.
pub struct CachedMetrics {
    source: Arc<dyn PressureSource>,
    cache: Mutex<Option<(Instant, Metrics)>>,
}

impl CachedMetrics {
    pub fn new(source: Arc<dyn PressureSource>) -> CachedMetrics {
        CachedMetrics { source, cache: Mutex::new(None) }
    }
}

impl MetricsSource for CachedMetrics {
    fn sample(&self) -> Option<Metrics> {
        // Held while sampling: concurrent callers share one read.
        let mut cache = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((at, metrics)) = cache.as_ref()
            && at.elapsed() < METRICS_CACHE
        {
            return Some(metrics.clone());
        }
        let metrics = metrics_from_pressure(&self.source.sample());
        *cache = Some((Instant::now(), metrics.clone()));
        Some(metrics)
    }
}

/// Metrics from a JSON file, re-read on every sample (`--admission-only`
/// tests): `{memory_total_mb, memory_available_mb, swap_total_mb,
/// swap_free_mb, memory_psi_full_avg10, memory_backing: null |
/// {total_bytes, available_bytes}}`. A missing or invalid file is no
/// metrics, which refuses admission with a node shape.
pub struct FileMetrics(pub PathBuf);

impl FileMetrics {
    pub fn parse(value: &Value) -> Option<Metrics> {
        let object = value.as_object()?;
        let number = |key: &str| object.get(key).and_then(Value::as_u64);
        let backing = match object.get("memory_backing") {
            None | Some(Value::Null) => None,
            Some(backing) => Some(MemoryBackingCapacity {
                total_bytes: backing.get("total_bytes").and_then(Value::as_u64),
                available_bytes: backing.get("available_bytes").and_then(Value::as_u64),
                identity: None,
            }),
        };
        Some(Metrics {
            memory_total_mb: number("memory_total_mb")?,
            memory_available_mb: number("memory_available_mb")?,
            swap_total_mb: number("swap_total_mb").unwrap_or(0),
            swap_free_mb: number("swap_free_mb").unwrap_or(0),
            memory_psi_full_avg10: object.get("memory_psi_full_avg10").and_then(Value::as_f64),
            memory_backing: backing,
        })
    }
}

impl MetricsSource for FileMetrics {
    fn sample(&self) -> Option<Metrics> {
        let bytes = std::fs::read(&self.0).ok()?;
        FileMetrics::parse(&serde_json::from_slice(&bytes).ok()?)
    }
}

/// Resident samples from the pause policy's sampler (phase 3b), placement
/// from the memory journal.
pub struct NodeObservations {
    sampler: OnceLock<Arc<ResidentSampler>>,
    modes: Option<Arc<MemoryBackingStore>>,
    ram_backing_configured: bool,
}

impl NodeObservations {
    pub fn new(modes: Option<Arc<MemoryBackingStore>>, ram_backing_configured: bool) -> NodeObservations {
        NodeObservations { sampler: OnceLock::new(), modes, ram_backing_configured }
    }

    /// The pause policy's sampler, once it runs. Without one every forecast
    /// charges its whole bound.
    pub fn set_sampler(&self, sampler: Arc<ResidentSampler>) {
        let _ = self.sampler.set(sampler);
    }
}

impl Observations for NodeObservations {
    fn historical(&self, key: &Owner) -> Option<ResidentSample> {
        self.sampler.get()?.historical(key)
    }

    /// `warden.application_memory_mode`: the allocation's placement, else
    /// RAM on a RAM-capable worker (conservative), else file.
    fn memory_mode(&self, key: &Owner) -> MemoryMode {
        match self.modes.as_ref().and_then(|modes| modes.active_mode(&key.0, key.1)) {
            Some(ActiveMode::Ram) => MemoryMode::Ram,
            Some(ActiveMode::File) => MemoryMode::File,
            None if self.ram_backing_configured => MemoryMode::Ram,
            None => MemoryMode::File,
        }
    }

    fn forget(&self, key: &Owner) {
        if let Some(sampler) = self.sampler.get() {
            sampler.forget(key);
        }
    }

    fn reload_mode(&self, key: &Owner) {
        if let Some(modes) = &self.modes {
            let _ = modes.observe_allocation(&key.0, key.1);
        }
    }
}

/// The agent is reachable when its socket accepts a connection.
pub struct AgentSocketProbe(pub PathBuf);

impl AgentProbe for AgentSocketProbe {
    fn reachable(&self) -> BoxFuture<bool> {
        let path = self.0.clone();
        Box::pin(async move {
            tokio::time::timeout(Duration::from_secs(1), tokio::net::UnixStream::connect(path)).await.is_ok_and(|connected| connected.is_ok())
        })
    }
}

/// The reclaim tick's demand from noded's own ledger (replacing
/// `agent-demand.json`): billed only while both the drain row and the
/// admission gate say open.
pub struct LedgerDemandSource {
    pub service: Arc<AdmissionService>,
    pub admission_open: Box<dyn Fn() -> bool + Send + Sync>,
}

impl DemandSource for LedgerDemandSource {
    fn demand(&self) -> MemoryDemand {
        if !(self.admission_open)() {
            return MemoryDemand::default();
        }
        self.service.reclaim_demand()
    }
}

/// Local waits' growth events into the ledger: every 50 ms, at most 4,096
/// per batch, one commit each. An item's error is logged and skipped, as
/// Python's `record_growth_events` does.
pub async fn apply_local_growth(service: Arc<AdmissionService>, mut events: mpsc::UnboundedReceiver<GrowthEvent>) {
    while let Some(first) = events.recv().await {
        tokio::time::sleep(GROWTH_BATCH_INTERVAL).await;
        let mut batch = vec![first];
        while batch.len() < GROWTH_BATCH_ITEMS {
            match events.try_recv() {
                Ok(event) => batch.push(event),
                Err(_) => break,
            }
        }
        let items = batch
            .iter()
            .map(|event| Event {
                action: match event.action {
                    WaitAction::Wait => EventAction::Wait,
                    WaitAction::Activate => EventAction::Activate,
                },
                owner: (event.sandbox_id.clone(), event.generation),
                request_id: event.request_id.clone(),
                job_id: String::new(),
                launch_sha256: String::new(),
            })
            .collect();
        match service.growth_events(items).await {
            Ok(results) => {
                for (event, result) in batch.iter().zip(results) {
                    if let Err(refusal) = result {
                        eprintln!("ucloud-noded: local wait growth bookkeeping for {} skipped: {}", event.sandbox_id, refusal.message());
                    }
                }
            }
            Err(refusal) => eprintln!("ucloud-noded: local wait growth batch of {} skipped: {}", batch.len(), refusal.message()),
        }
    }
}
