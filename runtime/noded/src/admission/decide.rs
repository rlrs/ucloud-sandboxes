//! The node admission decision: a port of `ucloud_sandboxes/admission_decision.py`
//! (`gate` and `decide`), which `DirectSandboxService._active_admission_guard`
//! drives. Neither has CPU checks (docs/admission-port-3c.md §0.9 and
//! decision G).
//!
//! One guard pass is two lock sections around a metrics sample:
//! 1. [`gate`] before sampling: the drain fence, the node shape, and a retry
//!    deadline that already passed;
//! 2. [`decide`] after sampling: the same fences, the live memory checks
//!    (`resource_admission._memory_pressure_error`), then the ledger
//!    projections against physical headroom above the 2 GiB floor and
//!    against RAM backing.
//!
//! The driver (3c.2) owns the rest of the loop: sampling outside the lock,
//! owner validation, requoting growth (`refresh_wait_cost` before `decide`,
//! when there is a node shape, a sample and a cost), refreshing aged
//! forecasts, and waiting out [`Decision::Wait`] on the admission condition
//! with `previous_error` set to its reason. Rules kept from Python:
//! - no node shape (`capacity == None`) means slot-only: grant;
//! - no metrics refuses at once;
//! - `memory_total == 0` is a projected wait;
//! - after a wait, a sample that arrives past the deadline refuses, even when
//!   it is low.

use super::Owner;
use super::ledger::{Cost, Kind, Ledger, Resource, Shape};
use crate::pause_policy::MemoryBackingCapacity;

/// `PHYSICAL_MEMORY_FLOOR_MB`: resident headroom kept for the node agent,
/// sentries and a new operation. Free swap never satisfies it.
pub const PHYSICAL_MEMORY_FLOOR_MB: u64 = 2048;
/// `admission_decision.RETRY_SECONDS`: a memory wait resamples once the
/// sampler's 200 ms cache expired.
pub const RETRY_SECONDS: f64 = 0.21;
/// `memory_psi_full_avg10` at or above this blocks admission.
pub const MEMORY_PSI_FULL_LIMIT: f64 = 10.0;

const MIB: u64 = 1024 * 1024;

pub const CLOSED: &str = "direct node admission is closed";
pub const SHAPE: &str = "sandbox CPU or memory request exceeds the physical node shape";
pub const NO_METRICS: &str = "direct node has no fresh runtime metrics for dynamic admission";
pub const MEMORY_PRESSURE: &str = "direct node memory pressure blocks active admission";
pub const PHYSICAL_HEADROOM: &str = "direct node has insufficient physical live memory headroom";
pub const LIVE_HEADROOM: &str = "direct node has insufficient live memory headroom";
pub const PROJECTED: &str = "node memory headroom is reserved by in-flight transitions";
pub const PROJECTED_RAM: &str = "node RAM backing headroom is reserved by in-flight transitions";

/// The `NodeRuntimeMetrics` fields admission reads.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Metrics {
    pub memory_total_mb: u64,
    pub memory_available_mb: u64,
    pub swap_total_mb: u64,
    pub swap_free_mb: u64,
    pub memory_psi_full_avg10: Option<f64>,
    /// `None`: no RAM backing sample (see [`Node::ram_backing_configured`]).
    pub memory_backing: Option<MemoryBackingCapacity>,
}

/// The node's side of a decision, read under the admission lock.
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    /// The drain gate.
    pub admission_open: bool,
    /// The active capacity shape; `None` disables pressure checks.
    pub capacity: Option<Shape>,
    /// `application_memory_root` is configured: absent backing evidence is
    /// 0 available, never unlimited.
    pub ram_backing_configured: bool,
}

/// Which optional checks apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checks {
    pub shape: bool,
    pub memory_psi: bool,
}

impl Default for Checks {
    fn default() -> Self {
        Checks { shape: true, memory_psi: true }
    }
}

/// One admission attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub node: Node,
    pub owner: Owner,
    /// The shape check's request (vcpu, memory_mb).
    pub requested: Shape,
    /// The transition's cost; `None` checks live pressure only.
    pub cost: Option<Cost>,
    pub checks: Checks,
    /// Monotonic seconds.
    pub deadline: f64,
    /// Monotonic seconds, read after the sample.
    pub now: f64,
    /// The reason the previous pass waited, if it did.
    pub previous_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// `SandboxAdmissionClosedError`.
    Closed(String),
    /// `SandboxCapacityUnavailableError`.
    Capacity(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    Grant,
    /// Keep the demand visible and wait up to `seconds` on the admission
    /// condition, then sample again.
    Wait { reason: String, seconds: f64 },
    Refuse(Refusal),
}

/// `dynamic_request_fits(requested, ResourceQuantity(), capacity)`: no disk
/// is available to a pressure-only check.
fn fits(requested: &Shape, capacity: &Shape) -> bool {
    requested.vcpu.is_finite()
        && requested.vcpu >= 0.0
        && (requested.vcpu > 0.0 || requested.memory_mb > 0 || requested.disk_mb > 0)
        && requested.vcpu <= capacity.vcpu
        && requested.memory_mb <= capacity.memory_mb
        && requested.disk_mb == 0
}

/// `_memory_pressure_error`.
pub fn memory_pressure_error(metrics: &Metrics, requested_memory_mb: u64, check_psi: bool) -> Option<&'static str> {
    if check_psi && metrics.memory_psi_full_avg10.is_some_and(|value| value >= MEMORY_PSI_FULL_LIMIT) {
        return Some(MEMORY_PRESSURE);
    }
    if metrics.memory_total_mb > 0 && metrics.memory_available_mb < PHYSICAL_MEMORY_FLOOR_MB {
        return Some(PHYSICAL_HEADROOM);
    }
    let minimum = PHYSICAL_MEMORY_FLOOR_MB.max(requested_memory_mb);
    let (available, known) = if metrics.swap_total_mb > 0 {
        (metrics.memory_available_mb + metrics.swap_free_mb, true)
    } else {
        (metrics.memory_available_mb, metrics.memory_total_mb > 0)
    };
    (known && available < minimum).then_some(LIVE_HEADROOM)
}

/// `dynamic_pressure_error(..., check_cpu=False)`.
fn pressure_error(metrics: Option<&Metrics>, requested_memory_mb: u64, check_psi: bool) -> Option<&'static str> {
    match metrics {
        None => Some(NO_METRICS),
        Some(metrics) => memory_pressure_error(metrics, requested_memory_mb, check_psi),
    }
}

/// The first lock section: fences that need no sample.
pub fn gate(request: &Request) -> Result<(), Refusal> {
    if !request.node.admission_open {
        return Err(Refusal::Closed(CLOSED.into()));
    }
    if let Some(capacity) = &request.node.capacity
        && request.checks.shape
        && !fits(&request.requested, capacity)
    {
        return Err(Refusal::Capacity(SHAPE.into()));
    }
    match &request.previous_error {
        Some(error) if request.now >= request.deadline => Err(Refusal::Capacity(error.clone())),
        _ => Ok(()),
    }
}

/// The second lock section, after sampling: grant, wait or refuse.
pub fn decide(metrics: Option<&Metrics>, ledger: &Ledger, request: &Request) -> Decision {
    if !request.node.admission_open {
        return Decision::Refuse(Refusal::Closed(CLOSED.into()));
    }
    let Some(capacity) = &request.node.capacity else {
        // Slot-only: no shape means no pressure source.
        return late(request).unwrap_or(Decision::Grant);
    };
    if request.checks.shape && !fits(&request.requested, capacity) {
        return Decision::Refuse(Refusal::Capacity(SHAPE.into()));
    }
    // Pressure is judged against the transition's own footprint when known.
    let requested_memory_mb = match request.cost.as_ref().and_then(|cost| cost.memory_bytes) {
        Some(bytes) => bytes.div_ceil(MIB),
        None => request.requested.memory_mb,
    };
    let mut error = pressure_error(metrics, requested_memory_mb, request.checks.memory_psi);
    let mut projected_error = false;
    if let (Some(cost), Some(metrics)) = (&request.cost, metrics) {
        // MemAvailable is shared by concurrent requests: each in-flight
        // footprint is reserved exactly once, above the physical floor.
        let headroom = if metrics.memory_total_mb > 0 {
            metrics.memory_available_mb.saturating_sub(PHYSICAL_MEMORY_FLOOR_MB) * MIB
        } else {
            0
        };
        let projected = ledger.projected_memory_bytes(&request.owner, cost, Resource::Memory);
        if metrics.memory_total_mb == 0 || projected > headroom {
            error = Some(PROJECTED);
            projected_error = true;
        }
        // File-backed restores spend physical headroom but no tmpfs blocks;
        // the RAM projection keeps every RAM owner's whole guarantee.
        let projected_ram = ledger.projected_memory_bytes(&request.owner, cost, Resource::RamBacking);
        let ram_available = match &metrics.memory_backing {
            Some(backing) => Some(backing.available_bytes.unwrap_or(0)),
            None if request.node.ram_backing_configured => Some(0),
            None => None,
        };
        if ram_available.is_some_and(|available| projected_ram > available) {
            error = Some(PROJECTED_RAM);
            projected_error = true;
        }
    }
    if let Some(refused) = late(request) {
        return refused;
    }
    let Some(error) = error else {
        return Decision::Grant;
    };
    let memory_wait = request.cost.as_ref().is_some_and(|cost| matches!(cost.kind, Kind::Startup | Kind::Restore))
        && metrics.is_some_and(|metrics| {
            metrics.memory_total_mb > 0 && memory_pressure_error(metrics, requested_memory_mb, request.checks.memory_psi).is_some()
        });
    if !(projected_error || memory_wait) {
        return Decision::Refuse(Refusal::Capacity(error.into()));
    }
    // Keep pending demand visible until headroom returns, within the deadline.
    if request.now + RETRY_SECONDS >= request.deadline {
        return Decision::Refuse(Refusal::Capacity(error.into()));
    }
    Decision::Wait { reason: error.into(), seconds: RETRY_SECONDS.min(request.deadline - request.now) }
}

/// A slow sampler must not grant after the retry deadline, even when its
/// late sample is low.
fn late(request: &Request) -> Option<Decision> {
    match &request.previous_error {
        Some(error) if request.now >= request.deadline => Some(Decision::Refuse(Refusal::Capacity(error.clone()))),
        _ => None,
    }
}
