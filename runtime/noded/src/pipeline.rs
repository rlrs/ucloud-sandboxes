//! The node's create configuration and the pipeline that runs a create.
//!
//! The Python agent parses the node flags and runs the assembly checks; the
//! daemon reads the effective configuration from it
//! (`GET /internal/v1/creates/config`) rather than duplicating flag plumbing.
//! Until that answer arrives every create goes to the agent.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hyper::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::agent_rpc::AgentClient;
use crate::create::{Admitted, CreateError, Pipeline};
use crate::timings::Timings;

#[derive(Debug, Clone, Deserialize)]
pub struct AllowTcp {
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnvironmentConfig {
    pub registry_url: String,
    pub registry_repository: String,
    pub backend_socket: PathBuf,
    #[serde(default)]
    pub rafs: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateConfig {
    pub rust_creates_enabled: bool,
    pub state_root: PathBuf,
    pub image_cache_root: PathBuf,
    pub volume_mount_root: PathBuf,
    pub storage_native_socket: PathBuf,
    pub runsc: PathBuf,
    pub runtime_root: PathBuf,
    pub bundle_root: PathBuf,
    pub journal_root: PathBuf,
    pub network: String,
    pub network_mtu: u32,
    /// Pre-created direct-egress pairs the daemon keeps; absent from agents
    /// older than the daemon's pool (then `network::DEFAULT_POOL_SIZE`).
    #[serde(default)]
    pub network_pool_size: Option<usize>,
    pub direct_network_allow_tcp: Vec<AllowTcp>,
    pub dns_named_egress: bool,
    pub relays_configured: bool,
    /// Direct egress's `/etc/hosts`: relay names at their virtual addresses
    /// (Python `relay_hosts`). Absent from agents older than 0.9.70, which
    /// leave the image's file.
    #[serde(default)]
    pub relay_hosts: BTreeMap<String, String>,
    pub split_memory_backing: bool,
    pub memory_backing_hard_capacity_bytes: u64,
    pub application_memory_root: Option<PathBuf>,
    pub reflink_memory_restore: bool,
    pub workspace_initial_grant_mb: u64,
    pub demonstrated_memory: bool,
    pub init_binary: PathBuf,
    pub managed_init_binary: PathBuf,
    pub environment: Option<EnvironmentConfig>,
    pub runtime_compatibility_sha256: String,
    pub node_epoch: String,
    /// Phase 2a; absent from agents older than it.
    #[serde(default)]
    pub exec: Option<ExecConfig>,
    /// Phase 3a; absent from agents older than it.
    #[serde(default)]
    pub pause: Option<PauseBlock>,
    /// Step 3c.0; absent from agents older than it.
    #[serde(default)]
    pub admission: Option<AdmissionConfig>,
    /// The agent's digest of this configuration (`config_sha256`).
    #[serde(default)]
    pub config_sha256: String,
}

/// The agent's node admission configuration (docs/admission-port-3c.md §3.3),
/// read from its live service. The RAM backing root is the top-level
/// `application_memory_root`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AdmissionConfig {
    /// The agent runs with `--rust-admission`.
    pub rust_admission_enabled: bool,
    /// Startup slots (`FairCapacity(max_concurrent_startups)`).
    pub max_concurrent_startups: usize,
    /// Restore slots (`FairCapacity(max_concurrent_restores)`).
    pub max_concurrent_restores: usize,
    /// The default wait for a slot and for capacity.
    pub admission_wait_seconds: f64,
    /// The active capacity shape; `None` admits on slots alone.
    pub active_capacity: Option<ActiveCapacity>,
    /// Physical headroom no admission may spend (`PHYSICAL_MEMORY_FLOOR_MB`).
    pub physical_memory_floor_mb: u64,
    /// How long a resident observation earns growth credit.
    pub growth_observation_max_age_seconds: f64,
    /// What a paused continuation's swapped bytes may cost at most
    /// (`pause_tier.PREFETCH_MAX_BYTES`).
    pub prefetch_max_bytes: u64,
    /// Publication lanes (`FairCapacity(16)`).
    pub publication_lanes: usize,
    /// The capacity wait's resample interval.
    pub retry_seconds: f64,
}

/// `ResourceQuantity(vcpu, memory_mb)` of the node's active capacity.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct ActiveCapacity {
    pub vcpu: f64,
    pub memory_mb: u64,
}

/// The agent's pause-tier configuration (phase 3a). Only the switch is read
/// here; the policy reads the rest.
#[derive(Debug, Clone, Deserialize)]
pub struct PauseBlock {
    pub rust_pause_enabled: bool,
    #[serde(flatten)]
    pub settings: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecSessionLimits {
    pub max_sessions: usize,
    pub max_events_per_session: usize,
    pub completed_retention_seconds: f64,
    pub delivered_grace_seconds: f64,
    pub output_idle_timeout_seconds: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecConfig {
    pub rust_execs_enabled: bool,
    pub runsc: PathBuf,
    pub runtime_root: PathBuf,
    pub warden_locks_dir: PathBuf,
    pub warden_paused_dir: PathBuf,
    pub active_capacity_configured: bool,
    pub memory_floor_mib: u64,
    pub sessions: ExecSessionLimits,
}

impl CreateConfig {
    /// Node-wide reasons the daemon cannot create at all.
    pub fn refusal(&self) -> Option<&'static str> {
        if !self.rust_creates_enabled {
            Some("the agent does not run with --rust-creates")
        } else if self.dns_named_egress {
            Some("DNS-named egress endpoints stay with the agent")
        } else if self.environment.is_none() {
            Some("the legacy image store stays with the agent")
        } else {
            None
        }
    }
}

/// Whether one request is within what the daemon creates (else the agent does):
/// direct egress only, and a known top-level shape.
pub fn spec_supported(spec: &Map<String, Value>) -> bool {
    match spec.get("network_policy") {
        None => {}
        Some(Value::Object(policy)) => {
            let direct = policy.iter().all(|(key, value)| key == "egress" && value == "direct");
            if !direct {
                return false;
            }
        }
        Some(_) => return false,
    }
    // A static management helper needs the `files ready` probe the agent runs.
    let helper = spec.get("filesystem").and_then(|fs| fs.get("management_helper"));
    helper.is_none_or(|helper| helper == "shell")
}

/// The pipeline once the configuration is known; creates go to the agent
/// until then, and for good once the agent reports another configuration.
pub struct LazyPipeline {
    inner: OnceLock<(Arc<dyn Pipeline>, String)>,
    stale: AtomicBool,
}

impl LazyPipeline {
    pub fn new() -> Arc<Self> {
        Arc::new(LazyPipeline { inner: OnceLock::new(), stale: AtomicBool::new(false) })
    }

    /// Ask the agent for its configuration until it is one the daemon
    /// serves, then build the pipeline with `build`. Refusals are re-polled:
    /// a re-initialized node restarts the agent after the daemon.
    pub async fn load(
        self: Arc<Self>,
        agent: Arc<AgentClient>,
        build: impl Fn(CreateConfig) -> Result<Arc<dyn Pipeline>, String>,
    ) {
        let mut last_refusal = String::new();
        loop {
            let refusal = match agent.call_json(Method::GET, "/internal/v1/creates/config", None).await {
                Ok((StatusCode::OK, value)) => {
                    let digest = value.get("config_sha256").and_then(Value::as_str).unwrap_or_default().to_string();
                    match serde_json::from_value::<CreateConfig>(value) {
                        Ok(config) => match config.refusal() {
                            Some(reason) => format!("creates stay with the agent: {reason}"),
                            None => match build(config) {
                                Ok(pipeline) => {
                                    let _ = self.inner.set((pipeline, digest));
                                    eprintln!("ucloud-noded: creating sandboxes");
                                    return;
                                }
                                Err(error) => format!("cannot create sandboxes ({error}); creates stay with the agent"),
                            },
                        },
                        Err(error) => format!("the agent's create configuration is unusable ({error})"),
                    }
                }
                Ok((status, _)) => format!("create configuration answered {status}"),
                Err(error) => format!("create configuration unavailable ({error})"),
            };
            if refusal != last_refusal {
                eprintln!("ucloud-noded: {refusal}; retrying");
                last_refusal = refusal;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

impl Pipeline for LazyPipeline {
    fn create<'a>(
        &'a self,
        admitted: &'a Admitted,
        timings: &'a mut Timings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, CreateError>> + Send + 'a>> {
        match self.inner.get() {
            Some((pipeline, _)) => pipeline.create(admitted, timings),
            None => Box::pin(async { Err(CreateError::Unsupported("the create pipeline is not ready".into())) }),
        }
    }

    fn supports(&self, spec: &Map<String, Value>) -> bool {
        !self.stale.load(Ordering::Relaxed) && self.inner.get().is_some_and(|(pipeline, _)| pipeline.supports(spec))
    }

    fn accepts_config(&self, config_sha256: &str) -> bool {
        let Some((_, loaded)) = self.inner.get() else { return false };
        if loaded == config_sha256 {
            return true;
        }
        if !self.stale.swap(true, Ordering::Relaxed) {
            eprintln!("ucloud-noded: the agent's create configuration changed; creates go to the agent until a restart");
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The admission block as `create_handoff.admission_config` writes it.
    #[test]
    fn the_admission_block_is_typed_and_optional() {
        let block = json!({
            "rust_admission_enabled": true, "max_concurrent_startups": 32, "max_concurrent_restores": 8,
            "admission_wait_seconds": 30.0, "active_capacity": {"vcpu": 64, "memory_mb": 262144},
            "physical_memory_floor_mb": 2048, "growth_observation_max_age_seconds": 30.0,
            "prefetch_max_bytes": 1073741824u64, "publication_lanes": 16, "retry_seconds": 0.21,
        });
        let parsed: AdmissionConfig = serde_json::from_value(block.clone()).unwrap();
        assert_eq!(parsed, AdmissionConfig {
            rust_admission_enabled: true,
            max_concurrent_startups: 32,
            max_concurrent_restores: 8,
            admission_wait_seconds: 30.0,
            active_capacity: Some(ActiveCapacity { vcpu: 64.0, memory_mb: 262144 }),
            physical_memory_floor_mb: 2048,
            growth_observation_max_age_seconds: 30.0,
            prefetch_max_bytes: 1 << 30,
            publication_lanes: 16,
            retry_seconds: 0.21,
        });
        let mut slots_only = block.clone();
        slots_only["active_capacity"] = Value::Null;
        assert_eq!(serde_json::from_value::<AdmissionConfig>(slots_only).unwrap().active_capacity, None);
        let mut fractional = block.clone();
        fractional["active_capacity"] = json!({"vcpu": 3.5, "memory_mb": 8192});
        assert_eq!(serde_json::from_value::<AdmissionConfig>(fractional).unwrap().active_capacity,
                   Some(ActiveCapacity { vcpu: 3.5, memory_mb: 8192 }));
        // A missing or mistyped field is an unusable configuration, not a default.
        let mut missing = block.clone();
        missing.as_object_mut().unwrap().remove("retry_seconds");
        assert!(serde_json::from_value::<AdmissionConfig>(missing).is_err());
        let mut negative = block.clone();
        negative["physical_memory_floor_mb"] = json!(-1);
        assert!(serde_json::from_value::<AdmissionConfig>(negative).is_err());

        // Kept with the configuration and its digest; agents older than 3c.0 send neither block nor change.
        let mut config = json!({
            "rust_creates_enabled": true, "state_root": "/s", "image_cache_root": "/c", "volume_mount_root": "/v",
            "storage_native_socket": "/n", "runsc": "/r", "runtime_root": "/rr", "bundle_root": "/b",
            "journal_root": "/j", "network": "none", "network_mtu": 1420, "direct_network_allow_tcp": [],
            "dns_named_egress": false, "relays_configured": false, "split_memory_backing": false,
            "memory_backing_hard_capacity_bytes": 0, "application_memory_root": null,
            "reflink_memory_restore": false, "workspace_initial_grant_mb": 0, "demonstrated_memory": false,
            "init_binary": "/i", "managed_init_binary": "/m", "environment": null,
            "runtime_compatibility_sha256": "a", "node_epoch": "e",
        });
        let older: CreateConfig = serde_json::from_value(config.clone()).unwrap();
        assert_eq!((older.admission, older.config_sha256.as_str()), (None, ""));
        config["admission"] = block;
        config["config_sha256"] = json!("d".repeat(64));
        let current: CreateConfig = serde_json::from_value(config).unwrap();
        assert_eq!(current.admission, Some(parsed));
        assert_eq!(current.config_sha256, "d".repeat(64));
    }

    #[test]
    fn only_direct_egress_and_the_shell_helper_are_supported() {
        let spec = |value: Value| value.as_object().unwrap().clone();
        assert!(spec_supported(&spec(json!({"id": "a"}))));
        assert!(spec_supported(&spec(json!({"network_policy": {}}))));
        assert!(spec_supported(&spec(json!({"network_policy": {"egress": "direct"}}))));
        assert!(!spec_supported(&spec(json!({"network_policy": {"egress": "relay", "relay": "default"}}))));
        assert!(!spec_supported(&spec(json!({"network_policy": null}))));
        assert!(spec_supported(&spec(json!({"filesystem": {"management_helper": "shell"}}))));
        assert!(!spec_supported(&spec(json!({"filesystem": {"management_helper": "static"}}))));
    }
}
