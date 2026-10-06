//! The node's create configuration and the pipeline that runs a create.
//!
//! The Python agent parses the node flags and runs the assembly checks; the
//! daemon reads the effective configuration from it
//! (`GET /internal/v1/creates/config`) rather than duplicating flag plumbing.
//! Until that answer arrives every create goes to the agent.

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
    pub direct_network_allow_tcp: Vec<AllowTcp>,
    pub dns_named_egress: bool,
    pub relays_configured: bool,
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
