//! noded's own callers of managed growth admission (decision C): the job
//! start's launch and bind, and terminal observation. [`AgentAdmission`] is
//! the agent's `/internal/v1/managed/{launch,bind,terminal}`, as before
//! step 3c.2; [`LocalAdmission`] is the in-process service. [`NodeAdmission`]
//! picks the in-process one only once the node runs its own admission
//! (`--rust-admission` and the agent's say-so).

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hyper::{Method, StatusCode};
use serde_json::{Value, json};

use super::service::{AdmissionService, BoxFuture, RECOVERING, Refusal};
use crate::agent_rpc::AgentClient;
use crate::node_pipeline::NodePipeline;

/// `ManagedPrimaryOwnedError`'s message.
const MANAGED_PRIMARY_OWNED: &str = "sandbox generation already owns another primary process";

/// Why a launch was not admitted.
#[derive(Debug, PartialEq, Eq)]
pub enum LaunchError {
    /// Queued for memory past the deadline: nothing was dispatched (503
    /// `node_startup_busy`).
    StartupBusy(String),
    /// Another launch owns this generation's primary.
    PrimaryOwned { message: String, job_id: String },
    /// Nothing was admitted here: the agent runs the start itself.
    Forward(&'static str),
}

#[derive(Debug, PartialEq, Eq)]
pub enum BindError {
    PrimaryOwned { message: String, job_id: String },
    Failed(String),
}

pub trait AdmissionPort: Send + Sync {
    /// Commit (and admit) the launch before its ambiguous dispatch. The
    /// committed intent's job ID; empty for an imported primary, which the
    /// caller binds after the dispatch.
    fn launch(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<String, LaunchError>>;
    fn bind(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<(), BindError>>;
    /// The first terminal record of this primary; an error is only logged.
    fn terminal(&self, sandbox_id: &str, generation: u64, job_id: &str) -> BoxFuture<Result<(), String>>;
}

/// The agent's growth ledger over its internal routes.
pub struct AgentAdmission {
    agent: Arc<AgentClient>,
}

impl AgentAdmission {
    pub fn new(agent: Arc<AgentClient>) -> AgentAdmission {
        AgentAdmission { agent }
    }
}

fn identity(sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> Value {
    json!({"sandbox_id": sandbox_id, "generation": generation, "job_id": job_id, "launch_sha256": launch_sha256})
}

impl AdmissionPort for AgentAdmission {
    fn launch(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<String, LaunchError>> {
        let agent = self.agent.clone();
        let body = identity(sandbox_id, generation, job_id, launch_sha256);
        Box::pin(async move {
            let Ok(launch) = agent.call(Method::POST, "/internal/v1/managed/launch", Some(&body)).await else {
                return Err(LaunchError::Forward("launch admission unavailable"));
            };
            let answer = launch.json().unwrap_or(Value::Null);
            let message = |default: &str| answer.get("error").and_then(Value::as_str).unwrap_or(default).to_string();
            match (launch.status.as_u16(), answer.get("error_code").and_then(Value::as_str)) {
                (200, _) => match answer.get("job_id").and_then(Value::as_str) {
                    Some(job_id) => Ok(job_id.to_string()),
                    None => Err(LaunchError::Forward("launch answered outside its contract")),
                },
                (503, Some("startup_busy")) => Err(LaunchError::StartupBusy(message("node startup capacity is busy"))),
                (409, Some("primary_owned")) => Err(LaunchError::PrimaryOwned {
                    message: message(MANAGED_PRIMARY_OWNED),
                    job_id: answer.get("job_id").and_then(Value::as_str).unwrap_or("").to_string(),
                }),
                _ => Err(LaunchError::Forward("launch declined")),
            }
        })
    }

    fn bind(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<(), BindError>> {
        let agent = self.agent.clone();
        let body = identity(sandbox_id, generation, job_id, launch_sha256);
        Box::pin(async move {
            match agent.call(Method::POST, "/internal/v1/managed/bind", Some(&body)).await {
                Ok(reply) if reply.status == StatusCode::OK => Ok(()),
                Ok(reply) => {
                    let answer = reply.json().unwrap_or(Value::Null);
                    let message = answer.get("error").and_then(Value::as_str).unwrap_or("managed process bind failed").to_string();
                    if reply.status == StatusCode::CONFLICT && answer.get("error_code").and_then(Value::as_str) == Some("primary_owned") {
                        let job_id = answer.get("job_id").and_then(Value::as_str).unwrap_or("").to_string();
                        return Err(BindError::PrimaryOwned { message, job_id });
                    }
                    Err(BindError::Failed(message))
                }
                Err(rpc) => Err(BindError::Failed(rpc.to_string())),
            }
        })
    }

    fn terminal(&self, sandbox_id: &str, generation: u64, job_id: &str) -> BoxFuture<Result<(), String>> {
        let agent = self.agent.clone();
        let body = json!({"sandbox_id": sandbox_id, "generation": generation, "job_id": job_id});
        Box::pin(async move {
            match agent.call_within(Duration::from_secs(30), Method::POST, "/internal/v1/managed/terminal", Some(&body)).await {
                Ok(reply) if reply.status == StatusCode::OK => Ok(()),
                Ok(reply) => Err(format!("answered {}: {}", reply.status, String::from_utf8_lossy(&reply.body))),
                Err(error) => Err(error.to_string()),
            }
        })
    }
}

/// noded's own admission service, in process.
#[derive(Clone)]
pub struct LocalAdmission {
    service: Arc<AdmissionService>,
}

impl LocalAdmission {
    pub fn new(service: Arc<AdmissionService>) -> LocalAdmission {
        LocalAdmission { service }
    }
}

impl AdmissionPort for LocalAdmission {
    fn launch(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<String, LaunchError>> {
        let service = self.service.clone();
        let (owner, job_id, launch_sha256) = ((sandbox_id.to_string(), generation), job_id.to_string(), launch_sha256.to_string());
        Box::pin(async move {
            match service.launch(&owner, &job_id, &launch_sha256).await {
                Ok(job_id) => Ok(job_id),
                Err(Refusal::Busy(message) | Refusal::Capacity(message)) => Err(LaunchError::StartupBusy(message)),
                Err(Refusal::Recovering) => Err(LaunchError::StartupBusy(RECOVERING.into())),
                Err(Refusal::PrimaryOwned(job_id)) => Err(LaunchError::PrimaryOwned { message: MANAGED_PRIMARY_OWNED.into(), job_id }),
                // As the agent's launch route declines: closed, a lost
                // incarnation, not managed. Its own start answers.
                Err(_) => Err(LaunchError::Forward("launch declined")),
            }
        })
    }

    fn bind(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<(), BindError>> {
        let service = self.service.clone();
        let (owner, job_id, launch_sha256) = ((sandbox_id.to_string(), generation), job_id.to_string(), launch_sha256.to_string());
        Box::pin(async move {
            match service.bind(&owner, &job_id, &launch_sha256).await {
                Ok(_) => Ok(()),
                Err(Refusal::PrimaryOwned(job_id)) => Err(BindError::PrimaryOwned { message: MANAGED_PRIMARY_OWNED.into(), job_id }),
                Err(refusal) => Err(BindError::Failed(refusal.message())),
            }
        })
    }

    fn terminal(&self, sandbox_id: &str, generation: u64, job_id: &str) -> BoxFuture<Result<(), String>> {
        let service = self.service.clone();
        let (owner, job_id) = ((sandbox_id.to_string(), generation), job_id.to_string());
        Box::pin(async move { service.terminal(&owner, &job_id).await.map(|_| ()).map_err(|refusal| refusal.message()) })
    }
}

/// The daemon's port: in process once the loaded node runs admission,
/// otherwise (and until it loads) the agent's.
pub struct NodeAdmission {
    node: Arc<OnceLock<Arc<NodePipeline>>>,
    agent: AgentAdmission,
}

impl NodeAdmission {
    pub fn new(node: Arc<OnceLock<Arc<NodePipeline>>>, agent: Arc<AgentClient>) -> NodeAdmission {
        NodeAdmission { node, agent: AgentAdmission::new(agent) }
    }

    fn local(&self) -> Option<LocalAdmission> {
        self.node.get().and_then(|node| node.admission().cloned()).map(LocalAdmission::new)
    }
}

impl AdmissionPort for NodeAdmission {
    fn launch(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<String, LaunchError>> {
        match self.local() {
            Some(local) => local.launch(sandbox_id, generation, job_id, launch_sha256),
            None => self.agent.launch(sandbox_id, generation, job_id, launch_sha256),
        }
    }

    fn bind(&self, sandbox_id: &str, generation: u64, job_id: &str, launch_sha256: &str) -> BoxFuture<Result<(), BindError>> {
        match self.local() {
            Some(local) => local.bind(sandbox_id, generation, job_id, launch_sha256),
            None => self.agent.bind(sandbox_id, generation, job_id, launch_sha256),
        }
    }

    fn terminal(&self, sandbox_id: &str, generation: u64, job_id: &str) -> BoxFuture<Result<(), String>> {
        match self.local() {
            Some(local) => local.terminal(sandbox_id, generation, job_id),
            None => self.agent.terminal(sandbox_id, generation, job_id),
        }
    }
}
