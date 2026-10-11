//! `ucloud-noded --admission-only --internal-unix P --registry R
//! --node-control-token-file T [--admission-config C] [--metrics-file M]`:
//! the admission service alone on the internal socket, over a registry this
//! process owns, for the agent's tests (step 3c.2).
//!
//! `C` is the agent's `admission` block as JSON, optionally with the
//! top-level `application_memory_root` beside it; without it, slots only
//! (no node shape). `M` replaces `/proc` as the metrics source (see
//! [`super::sources::FileMetrics`]). There are no resident samples, so
//! active forecasts charge whole bounds, and no agent to probe: recovery
//! ends at the agent's hello or after the grace.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::service::{AdmissionService, MetricsSource, ServiceConfig, Sources};
use super::sources::{CachedMetrics, FileMetrics, NodeObservations};
use crate::internal::{self, AdmissionAccess, Internal, NodeView};
use crate::pause::prefetch::PREFETCH_MAX_BYTES;
use crate::pause_policy::{Clock, PressureSampler, SystemClock};
use crate::pipeline::AdmissionConfig;
use crate::registry::Registry;

pub struct Options {
    pub socket: PathBuf,
    pub registry: PathBuf,
    pub token: String,
    pub config: Option<PathBuf>,
    pub metrics: Option<PathBuf>,
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

struct StandaloneNode(Arc<AdmissionService>);

impl NodeView for StandaloneNode {
    fn admission_open(&self) -> Option<bool> {
        Some(self.0.admission_open())
    }

    fn config_sha256(&self) -> Option<String> {
        None
    }

    fn admission(&self) -> AdmissionAccess {
        AdmissionAccess::Ready(self.0.clone())
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
    let sources = Sources {
        store: registry.clone(),
        owners: registry.clone(),
        metrics,
        observations: Arc::new(NodeObservations::new(None, ram_root.is_some())),
        clock,
    };
    let service = AdmissionService::open(config, drain.admission_open, sources)?;
    service.start_recovery(None);
    let internal = Internal::new(session, &options.token, internal::own_uid(), Arc::new(StandaloneNode(service)))?;
    let listener = internal::bind(&options.socket).map_err(|error| format!("cannot listen on {}: {error}", options.socket.display()))?;
    eprintln!("ucloud-noded: serving admission only on unix:{}", options.socket.display());
    internal::serve(listener, internal, Duration::from_secs(5), shutdown).await;
    registry.close();
    Ok(())
}
