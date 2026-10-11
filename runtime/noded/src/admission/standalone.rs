//! `ucloud-noded --admission-only --internal-unix P --registry R
//! --node-control-token-file T [--admission-config C] [--metrics-file M]
//! [--observations-file O]`: the admission service alone on the internal
//! socket, over a registry this process owns, for the agent's tests (steps
//! 3c.2 and 3c.3).
//!
//! `C` is the agent's `admission` block as JSON, optionally with the
//! top-level `application_memory_root` beside it; without it, slots only
//! (no node shape). `M` replaces `/proc` as the metrics source (see
//! [`super::sources::FileMetrics`]). `O` replaces the resident sampler and
//! the memory journal (see [`FileObservations`]); without it there are no
//! resident samples, so active forecasts charge whole bounds. There is no
//! agent to probe: recovery ends at the agent's hello or after the grace.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;

use super::Owner;
use super::growth::{MemoryMode, Observations};
use super::service::{AdmissionService, MetricsSource, ServiceConfig, Sources};
use super::sources::{CachedMetrics, FileMetrics, NodeObservations};
use crate::internal::{self, AdmissionAccess, Internal, NodeView};
use crate::pause::prefetch::PREFETCH_MAX_BYTES;
use crate::pause_policy::resident::ResidentSample;
use crate::pause_policy::{Clock, PressureSampler, SystemClock};
use crate::pipeline::AdmissionConfig;
use crate::registry::{Registry, is_sandbox_id};

/// How often the observations file is checked between requests.
const OBSERVATIONS_POLL: Duration = Duration::from_millis(20);

pub struct Options {
    pub socket: PathBuf,
    pub registry: PathBuf,
    pub token: String,
    pub config: Option<PathBuf>,
    pub metrics: Option<PathBuf>,
    pub observations: Option<PathBuf>,
}

/// The block an agent without overrides sends, minus the node shape.
pub fn default_block() -> AdmissionConfig {
    AdmissionConfig {
        rust_admission_enabled: true,
        max_concurrent_startups: 4,
        max_concurrent_restores: 4,
        admission_wait_seconds: 30.0,
        active_capacity: None,
        physical_memory_floor_mb: super::decide::PHYSICAL_MEMORY_FLOOR_MB,
        growth_observation_max_age_seconds: super::growth::GROWTH_OBSERVATION_MAX_AGE_SECONDS,
        prefetch_max_bytes: PREFETCH_MAX_BYTES,
        publication_lanes: 16,
        retry_seconds: super::decide::RETRY_SECONDS,
    }
}

#[derive(Default)]
struct Loaded {
    /// The file's bytes as last read: unchanged bytes are not parsed again.
    bytes: Vec<u8>,
    /// The `seq` of the last sampler pass the service was told about.
    passed: Option<u64>,
    seq: Option<u64>,
    default_mode: Option<MemoryMode>,
    modes: HashMap<Owner, MemoryMode>,
    samples: HashMap<Owner, ResidentSample>,
}

/// Resident samples and memory placement from a JSON file, the stand-in for
/// the pause policy's sampler and the memory journal (`--observations-file`):
///
/// `{seq, default_memory_mode: "ram" | "file" | null, memory_modes:
/// [{sandbox_id, generation, mode}], samples: [{sandbox_id, generation,
/// <every ResidentMemorySample field>}]}`
///
/// The file is re-read whenever its bytes change: before each request, every
/// 20 ms, and before each quote (as the journal is). A new `seq` is a sampler
/// pass: the service prunes and refreshes its forecasts
/// ([`AdmissionService::after_resident_pass`]). A missing or invalid file
/// keeps what was last read. A sandbox without a mode is `default_memory_mode`,
/// else RAM where RAM backing is configured, else file (as in the daemon).
pub struct FileObservations {
    path: PathBuf,
    ram_backing_configured: bool,
    state: Mutex<Loaded>,
}

fn mode(text: &str) -> Option<MemoryMode> {
    MemoryMode::parse(text)
}

fn key_of(item: &serde_json::Map<String, Value>) -> Option<Owner> {
    let sandbox_id = item.get("sandbox_id")?.as_str().filter(|id| is_sandbox_id(id))?;
    let generation = item.get("generation")?.as_u64().filter(|generation| *generation >= 1)?;
    Some((sandbox_id.to_string(), generation))
}

fn sample_of(item: &serde_json::Map<String, Value>) -> Option<ResidentSample> {
    let count = |name: &str| item.get(name).and_then(Value::as_u64);
    let optional = |name: &str| match item.get(name) {
        None => Some(0),
        Some(value) => value.as_u64(),
    };
    Some(ResidentSample {
        current_bytes: count("current_bytes")?,
        anonymous_bytes: count("anonymous_bytes")?,
        file_bytes: count("file_bytes")?,
        dirty_bytes: count("dirty_bytes")?,
        writeback_bytes: count("writeback_bytes")?,
        refault_file_pages: count("refault_file_pages")?,
        cgroup_path: item.get("cgroup_path")?.as_str()?.to_string(),
        cgroup_device: count("cgroup_device")?,
        cgroup_inode: count("cgroup_inode")?,
        sentry_pid: u32::try_from(count("sentry_pid")?).ok()?,
        sentry_start_time_ticks: count("sentry_start_time_ticks")?,
        sampled_at: item.get("sampled_at")?.as_f64().filter(|at| at.is_finite())?,
        shared_memory_bytes: optional("shared_memory_bytes")?,
        peak_bytes: optional("peak_bytes")?,
        swap_bytes: optional("swap_bytes")?,
    })
}

impl FileObservations {
    pub fn new(path: PathBuf, ram_backing_configured: bool) -> FileObservations {
        FileObservations { path, ram_backing_configured, state: Mutex::new(Loaded::default()) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Loaded> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Re-read the file if it changed. Blocking.
    fn reload(&self) {
        let Ok(bytes) = std::fs::read(&self.path) else { return };
        let mut state = self.lock();
        if state.bytes == bytes {
            return;
        }
        let Some(document) = serde_json::from_slice::<Value>(&bytes).ok().and_then(|value| value.as_object().cloned()) else {
            return;
        };
        let items = |name: &str| -> Vec<serde_json::Map<String, Value>> {
            document.get(name).and_then(Value::as_array).map(|items| items.iter().filter_map(|item| item.as_object().cloned()).collect()).unwrap_or_default()
        };
        state.seq = document.get("seq").and_then(Value::as_u64);
        state.default_mode = document.get("default_memory_mode").and_then(Value::as_str).and_then(mode);
        state.modes = items("memory_modes")
            .iter()
            .filter_map(|item| Some((key_of(item)?, mode(item.get("mode")?.as_str()?)?)))
            .collect();
        state.samples = items("samples").iter().filter_map(|item| Some((key_of(item)?, sample_of(item)?))).collect();
        state.bytes = bytes;
    }

    /// Re-read, and whether a new sampler pass arrived since the last call.
    pub fn take_pass(&self) -> bool {
        self.reload();
        let mut state = self.lock();
        if state.seq.is_some() && state.seq != state.passed {
            state.passed = state.seq;
            return true;
        }
        false
    }
}

impl Observations for FileObservations {
    fn historical(&self, key: &Owner) -> Option<ResidentSample> {
        self.lock().samples.get(key).cloned()
    }

    fn memory_mode(&self, key: &Owner) -> MemoryMode {
        let state = self.lock();
        match state.modes.get(key).copied().or(state.default_mode) {
            Some(mode) => mode,
            None if self.ram_backing_configured => MemoryMode::Ram,
            None => MemoryMode::File,
        }
    }

    /// Until the file's next change, as the sampler forgets until its next pass.
    fn forget(&self, key: &Owner) {
        self.lock().samples.remove(key);
    }

    fn reload_mode(&self, _key: &Owner) {
        self.reload();
    }
}

struct StandaloneNode {
    service: Arc<AdmissionService>,
    observations: Option<Arc<FileObservations>>,
    /// One pass at a time: a request that finds the poller applying the
    /// file's pass waits for it.
    syncing: Mutex<()>,
}

impl StandaloneNode {
    /// A new pass in the observations file reaches the service before the
    /// request that follows it is served.
    fn sync(&self) {
        if let Some(observations) = &self.observations {
            let _one = self.syncing.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if observations.take_pass() {
                self.service.after_resident_pass();
            }
        }
    }
}

impl NodeView for StandaloneNode {
    fn admission_open(&self) -> Option<bool> {
        Some(self.service.admission_open())
    }

    fn config_sha256(&self) -> Option<String> {
        None
    }

    fn admission(&self) -> AdmissionAccess {
        self.sync();
        AdmissionAccess::Ready(self.service.clone())
    }
}

/// Serve until `shutdown` resolves.
pub async fn serve(options: Options, session: &str, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    let (block, ram_root) = match &options.config {
        None => (default_block(), None),
        Some(path) => {
            let text = std::fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            let mut value: Value = serde_json::from_slice(&text).map_err(|error| format!("{}: {error}", path.display()))?;
            let ram_root = value.as_object_mut().and_then(|object| object.remove("application_memory_root"));
            let ram_root = ram_root.and_then(|root| root.as_str().map(PathBuf::from));
            let block: AdmissionConfig = serde_json::from_value(value).map_err(|error| format!("{}: {error}", path.display()))?;
            (block, ram_root)
        }
    };
    let config = ServiceConfig::from_block(&block, ram_root.is_some())?;
    let registry = Arc::new(Registry::owner(&options.registry, 0).map_err(|error| format!("registry: {error}"))?);
    let drain = registry.load_drain().map_err(|error| format!("registry: {error}"))?;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());
    let metrics: Arc<dyn MetricsSource> = match options.metrics {
        Some(path) => Arc::new(FileMetrics(path)),
        None => Arc::new(CachedMetrics::new(Arc::new(PressureSampler::new(PathBuf::from("/proc"), ram_root.clone(), clock.clone())))),
    };
    let file_observations = options.observations.map(|path| Arc::new(FileObservations::new(path, ram_root.is_some())));
    if let Some(observations) = &file_observations {
        observations.take_pass();
    }
    let observations: Arc<dyn Observations + Send + Sync> = match &file_observations {
        Some(observations) => observations.clone(),
        None => Arc::new(NodeObservations::new(None, ram_root.is_some())),
    };
    let sources = Sources { store: registry.clone(), owners: registry.clone(), metrics, observations, clock };
    let service = AdmissionService::open(config, drain.admission_open, sources)?;
    service.start_recovery(None);
    let node = Arc::new(StandaloneNode { service: service.clone(), observations: file_observations, syncing: Mutex::new(()) });
    if node.observations.is_some() {
        let poller = Arc::downgrade(&node);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(OBSERVATIONS_POLL).await;
                let Some(node) = poller.upgrade() else { return };
                if tokio::task::spawn_blocking(move || node.sync()).await.is_err() {
                    return;
                }
            }
        });
    }
    let internal = Internal::new(session, &options.token, internal::own_uid(), node)?;
    let listener = internal::bind(&options.socket).map_err(|error| format!("cannot listen on {}: {error}", options.socket.display()))?;
    eprintln!("ucloud-noded: serving admission only on unix:{}", options.socket.display());
    internal::serve(listener, internal, Duration::from_secs(5), shutdown).await;
    registry.close();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_observations_file_is_samples_modes_and_passes() {
        let directory = std::env::temp_dir().join(format!("ucloud-noded-observations-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("observations.json");
        let observations = FileObservations::new(path.clone(), false);
        let key: Owner = ("one".into(), 7);
        assert!(!observations.take_pass(), "no file is no pass");
        assert_eq!(observations.memory_mode(&key), MemoryMode::File);
        let sample = serde_json::json!({
            "sandbox_id": "one", "generation": 7, "current_bytes": 3, "anonymous_bytes": 0, "file_bytes": 3,
            "dirty_bytes": 0, "writeback_bytes": 0, "refault_file_pages": 0, "cgroup_path": "/owned",
            "cgroup_device": 1, "cgroup_inode": 2, "sentry_pid": 10, "sentry_start_time_ticks": 20,
            "sampled_at": 12.5, "shared_memory_bytes": 2, "peak_bytes": 4,
        });
        let document = serde_json::json!({
            "seq": 1, "default_memory_mode": "ram",
            "memory_modes": [{"sandbox_id": "one", "generation": 7, "mode": "file"}], "samples": [sample],
        });
        std::fs::write(&path, document.to_string()).unwrap();
        assert!(observations.take_pass());
        assert!(!observations.take_pass(), "one pass per seq");
        assert_eq!(observations.memory_mode(&key), MemoryMode::File);
        assert_eq!(observations.memory_mode(&("two".into(), 1)), MemoryMode::Ram);
        let loaded = observations.historical(&key).unwrap();
        assert_eq!((loaded.current_bytes, loaded.peak_bytes, loaded.swap_bytes, loaded.sampled_at), (3, 4, 0, 12.5));
        observations.forget(&key);
        assert_eq!(observations.historical(&key), None);
        std::fs::write(&path, "not json").unwrap();
        assert!(!observations.take_pass(), "an invalid file keeps the last one");
        assert_eq!(observations.memory_mode(&key), MemoryMode::File);
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
