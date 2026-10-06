//! Phase 3a in the daemon: the pause policy (idle pause, reclaim, escalation
//! decisions, status.json) and local model waits, wired to the agent for the
//! rare commands Python still executes (escalation, the wake fallback) and the
//! growth events its ledger applies.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use hyper::{Method, StatusCode};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::agent_rpc::AgentClient;
use crate::local_wait::{GrowthEvent, LocalWaitConfig, LocalWaitEvents, LocalWaits, RegistryCandidates, WaitFence};
use crate::pause::PauseTier;
use crate::pause_policy::{BoxFuture, Escalated, Escalator, PauseFence, PausePolicy, PausePolicyHandle, PolicyConfig};
use crate::registry::Registry;
use crate::warden::Sandbox;

/// `POST /internal/v1/pauses/escalate`: Python parks the paused wait.
struct AgentEscalator {
    agent: Arc<AgentClient>,
}

impl Escalator for AgentEscalator {
    fn escalate(&self, sandbox: &Sandbox) -> BoxFuture<Result<Escalated, String>> {
        let agent = self.agent.clone();
        let body = json!({"sandbox_id": sandbox.sandbox_id, "generation": sandbox.generation});
        Box::pin(async move {
            // A capture takes seconds; the agent answers when it is done.
            let reply = agent
                .call_within(Duration::from_secs(600), Method::POST, "/internal/v1/pauses/escalate", Some(&body))
                .await
                .map_err(|error| error.to_string())?;
            let answer = reply.json().map_err(|error| error.to_string())?;
            match (reply.status, answer.get("state").and_then(Value::as_str)) {
                (StatusCode::OK, Some("parked")) => Ok(Escalated::Parked),
                (StatusCode::OK, Some("running")) => Ok(Escalated::Running),
                _ => Err(format!("escalation answered {}: {answer}", reply.status)),
            }
        })
    }
}

/// Growth events go to the agent in batches, off the pause and thaw paths.
struct AgentEvents {
    agent: Arc<AgentClient>,
    growth: mpsc::UnboundedSender<GrowthEvent>,
    /// Weak: the policy holds local waits (through `answered`), which hold these events.
    policy: Arc<OnceLock<Weak<PausePolicyHandle>>>,
}

impl LocalWaitEvents for AgentEvents {
    fn growth(&self, event: GrowthEvent) {
        let _ = self.growth.send(event);
    }

    fn wake(&self, sandbox_id: String, generation: u64, operation_id: String) -> crate::local_wait::BoxFuture<Result<(), String>> {
        let agent = self.agent.clone();
        Box::pin(async move {
            let body = json!({"generation": generation, "operation_id": operation_id});
            let path = format!("/v1/sandboxes/{sandbox_id}/wake");
            let reply = agent.call(Method::POST, &path, Some(&body)).await.map_err(|error| error.to_string())?;
            if reply.status.is_success() {
                Ok(())
            } else {
                Err(format!("wake answered {}: {}", reply.status, String::from_utf8_lossy(&reply.body)))
            }
        })
    }

    fn paused(&self, sandbox_id: &str, generation: u64, request_id: &str) {
        if let Some(policy) = self.policy.get().and_then(Weak::upgrade) {
            policy.record_pause(sandbox_id, generation, None, request_id);
        }
    }
}

/// Send queued growth events every 50 ms, at most 4,096 per request. A failed
/// batch is logged and dropped, as Python's queue does (risk 9).
async fn send_growth(agent: Arc<AgentClient>, mut events: mpsc::UnboundedReceiver<GrowthEvent>) {
    while let Some(first) = events.recv().await {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut batch = vec![first];
        while batch.len() < 4096 {
            match events.try_recv() {
                Ok(event) => batch.push(event),
                Err(_) => break,
            }
        }
        let body = json!({"items": batch.iter().map(GrowthEvent::to_json).collect::<Vec<_>>()});
        match agent.call(Method::POST, "/internal/v1/growth/events", Some(&body)).await {
            Ok(reply) if reply.status == StatusCode::OK => {}
            Ok(reply) => eprintln!("ucloud-noded: growth events answered {}", reply.status),
            Err(error) => eprintln!("ucloud-noded: growth events not delivered: {error}"),
        }
    }
}

/// The running pause tier. `stop` ends local waits (removing the nft table)
/// and the policy's loops; the daemon calls it on shutdown.
pub struct PauseRuntime {
    policy: Mutex<Option<Arc<PausePolicyHandle>>>,
    waits: Mutex<Option<Arc<LocalWaits>>>,
}

pub struct PauseInputs {
    pub agent: Arc<AgentClient>,
    pub tier: Arc<PauseTier>,
    pub registry: Arc<Registry>,
    pub state_root: PathBuf,
    pub warden_locks_dir: PathBuf,
    pub cgroup_root: PathBuf,
    /// The agent's `pause` block.
    pub settings: serde_json::Map<String, Value>,
}

impl PauseRuntime {
    /// Start inside the tokio runtime. Local waits fail closed: without them
    /// the policy still runs and nothing pauses on model calls.
    pub fn start(inputs: PauseInputs) -> PauseRuntime {
        let PauseInputs { agent, tier, registry, state_root, warden_locks_dir, cgroup_root, settings } = inputs;
        let idle = settings.get("idle_park_seconds").and_then(Value::as_f64).unwrap_or(0.0);
        let config = PolicyConfig::new(state_root.clone(), agent.session().to_string(), idle, tier.config());
        let policy = Arc::new(PausePolicy::start(
            config,
            tier.clone(),
            PauseFence::new(&warden_locks_dir),
            registry.clone(),
            Arc::new(AgentEscalator { agent: agent.clone() }),
        ));
        let local = settings.get("local_waits").and_then(Value::as_object);
        let waits = local.filter(|local| local.get("enabled").and_then(Value::as_bool).unwrap_or(false)).and_then(|local| {
            let relays: Vec<(String, u16)> = local
                .get("endpoints")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|endpoint| {
                    let ip = endpoint.get("ip")?.as_str()?.to_string();
                    let port = u16::try_from(endpoint.get("port")?.as_u64()?).ok()?;
                    Some((ip, port))
                })
                .collect();
            let candidates = Arc::new(RegistryCandidates::new(
                registry.clone(),
                state_root.join("network-slots.json"),
                crate::local_wait::NETWORK_CIDR,
                cgroup_root.clone(),
            ));
            let (growth, queue) = mpsc::unbounded_channel();
            tokio::spawn(send_growth(agent.clone(), queue));
            let cell = Arc::new(OnceLock::new());
            let _ = cell.set(Arc::downgrade(&policy));
            let events = Arc::new(AgentEvents { agent: agent.clone(), growth, policy: cell });
            match LocalWaits::start(LocalWaitConfig::new(relays, candidates), (*tier).clone(), WaitFence::new(&warden_locks_dir), events) {
                Ok(waits) => Some(Arc::new(waits)),
                Err(error) => {
                    eprintln!("ucloud-noded: local model waits are off: {error:?}");
                    None
                }
            }
        });
        if let Some(waits) = waits.as_ref().map(Arc::downgrade) {
            policy.set_answered(Arc::new(move |sandbox_id: &str, generation: u64| {
                waits.upgrade().is_some_and(|waits| waits.answered(sandbox_id, generation))
            }));
        }
        eprintln!("ucloud-noded: pause tier running (local waits {})", if waits.is_some() { "on" } else { "off" });
        PauseRuntime { policy: Mutex::new(Some(policy)), waits: Mutex::new(waits) }
    }

    /// Stop local waits, then the policy's loops (in-flight reclaims stop at
    /// their next window). Idempotent.
    pub async fn stop(&self) {
        let waits = self.waits.lock().expect("not poisoned").take();
        if let Some(waits) = waits {
            tokio::task::spawn_blocking(move || waits.stop()).await.ok();
        }
        let policy = self.policy.lock().expect("not poisoned").take();
        if let Some(policy) = policy {
            match Arc::try_unwrap(policy) {
                Ok(policy) => policy.stop().await,
                Err(_) => eprintln!("ucloud-noded: the pause policy is still referenced at shutdown"),
            }
        }
    }
}
