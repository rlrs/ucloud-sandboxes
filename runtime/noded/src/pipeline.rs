//! The node's create configuration and the pipeline that runs a create.
//!
//! The Python agent parses the node flags and runs the assembly checks; the
//! daemon reads the effective configuration from it
//! (`GET /internal/v1/creates/config`) rather than duplicating flag plumbing.
//! Until that answer arrives every create goes to the agent.

use std::path::PathBuf;
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

/// The pipeline once the configuration is known; creates go to the agent until then.
pub struct LazyPipeline {
    inner: OnceLock<Arc<dyn Pipeline>>,
}

impl LazyPipeline {
    pub fn new() -> Arc<Self> {
        Arc::new(LazyPipeline { inner: OnceLock::new() })
    }

    /// Ask the agent for its configuration until it answers, then build the
    /// pipeline with `build`. A node the daemon cannot serve keeps forwarding.
    pub async fn load(
        self: Arc<Self>,
        agent: Arc<AgentClient>,
        build: impl FnOnce(CreateConfig) -> Result<Arc<dyn Pipeline>, String>,
    ) {
        let config = loop {
            match agent.call_json(Method::GET, "/internal/v1/creates/config", None).await {
                Ok((StatusCode::OK, value)) => match serde_json::from_value::<CreateConfig>(value) {
                    Ok(config) => break config,
                    Err(error) => {
                        eprintln!("ucloud-noded: the agent's create configuration is unusable ({error}); creates stay with the agent");
                        return;
                    }
                },
                Ok((status, _)) => eprintln!("ucloud-noded: create configuration answered {status}; retrying"),
                Err(error) => eprintln!("ucloud-noded: create configuration unavailable ({error}); retrying"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        if let Some(reason) = config.refusal() {
            eprintln!("ucloud-noded: creates stay with the agent: {reason}");
            return;
        }
        match build(config) {
            Ok(pipeline) => {
                let _ = self.inner.set(pipeline);
                eprintln!("ucloud-noded: creating sandboxes");
            }
            Err(error) => eprintln!("ucloud-noded: cannot create sandboxes ({error}); creates stay with the agent"),
        }
    }
}

impl Pipeline for LazyPipeline {
    fn create<'a>(
        &'a self,
        admitted: &'a Admitted,
        timings: &'a mut Timings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), CreateError>> + Send + 'a>> {
        match self.inner.get() {
            Some(pipeline) => pipeline.create(admitted, timings),
            None => Box::pin(async { Err(CreateError::Unavailable("the create pipeline is not ready".into())) }),
        }
    }

    fn supports(&self, spec: &Map<String, Value>) -> bool {
        self.inner.get().is_some_and(|pipeline| pipeline.supports(spec))
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
