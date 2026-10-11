"""The node admission decision, without locks, clocks or waits.

``DirectSandboxService._active_admission_guard`` drives it: one guard pass is
two sections under the capacity lock around a metrics sample.

1. ``gate`` before sampling: the drain fence, the node shape, and a retry
   deadline that already passed.
2. ``decide`` after sampling: the same fences, the live memory checks, then the
   ledger projections against physical headroom above the 2 GiB floor and
   against RAM backing.

The driver owns everything else: sampling outside the lock, owner validation,
requoting growth and refreshing aged forecasts before ``decide``, and waiting
out a ``Wait`` on the admission condition with ``previous_error`` set to its
reason. ``runtime/noded/src/admission/decide.rs`` is the Rust port, checked
against goldens recorded from this module (docs/admission-port-3c.md, 3c.1).

CPU is not part of node admission (decision G): existing sandboxes share CPU
through their cgroups, and a sampled spike must not send a queued transition
to another worker.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import NodeRuntimeMetrics, ResourceQuantity
from .resource_admission import PHYSICAL_MEMORY_FLOOR_MB, dynamic_pressure_error, dynamic_request_fits
from .sandbox import SandboxAdmissionClosedError, SandboxCapacityUnavailableError
from .transition_admission import TransitionCost, TransitionKind, TransitionLedger

# A memory wait resamples once the production sampler's 200 ms cache expired.
RETRY_SECONDS = 0.21

CLOSED = "direct node admission is closed"
SHAPE = "sandbox CPU or memory request exceeds the physical node shape"
PROJECTED = "node memory headroom is reserved by in-flight transitions"
PROJECTED_RAM = "node RAM backing headroom is reserved by in-flight transitions"

_MIB = 1024**2


@dataclass(frozen=True)
class AdmissionNode:
    """The node's side of a decision, read under the admission lock."""

    admission_open: bool
    # The active capacity shape; None means slot-only (no pressure source).
    capacity: ResourceQuantity | None
    # application_memory_root is configured: absent RAM backing evidence is
    # 0 available, never unlimited.
    ram_backing_configured: bool = False


@dataclass(frozen=True)
class AdmissionRequest:
    """One admission attempt."""

    node: AdmissionNode
    owner: tuple[str, int] | None
    # The shape check's request (vcpu, memory_mb).
    requested: ResourceQuantity
    # The transition's cost; None checks live pressure only.
    cost: TransitionCost | None
    check_shape: bool
    check_memory_pressure: bool
    # Monotonic seconds.
    deadline: float
    # Monotonic seconds, read after the sample.
    now: float
    # The reason the previous pass waited, if it did.
    previous_error: str | None = None


@dataclass(frozen=True)
class Grant:
    pass


@dataclass(frozen=True)
class Wait:
    """Keep the demand visible; wait up to ``seconds``, then sample again."""

    reason: str
    seconds: float


@dataclass(frozen=True)
class Refuse:
    reason: str
    closed: bool = False

    def error(self) -> RuntimeError:
        if self.closed:
            return SandboxAdmissionClosedError(self.reason)
        return SandboxCapacityUnavailableError(self.reason)


Decision = Grant | Wait | Refuse


def fences(request: AdmissionRequest) -> Refuse | None:
    """The fences every lock section checks first: drain, then node shape."""

    if not request.node.admission_open:
        return Refuse(CLOSED, closed=True)
    capacity = request.node.capacity
    if capacity is not None and request.check_shape:
        if not dynamic_request_fits(request.requested, ResourceQuantity(), capacity):
            return Refuse(SHAPE)
    return None


def gate(request: AdmissionRequest) -> Refuse | None:
    """The first lock section: fences that need no sample."""

    return fences(request) or _late(request)


def decide(
    metrics: NodeRuntimeMetrics | None,
    ledger: TransitionLedger,
    request: AdmissionRequest,
) -> Decision:
    """The second lock section, after sampling: grant, wait or refuse."""

    refused = fences(request)
    if refused is not None:
        return refused
    capacity, cost = request.node.capacity, request.cost
    pressure_request = request.requested
    if cost is not None and cost.memory_bytes is not None:
        # Pressure is judged against the transition's own footprint when known.
        pressure_request = ResourceQuantity(
            vcpu=request.requested.vcpu, memory_mb=(cost.memory_bytes + _MIB - 1) // _MIB)
    pressure_error = (
        dynamic_pressure_error(
            metrics, pressure_request, check_cpu=False, check_memory_pressure=request.check_memory_pressure)
        if capacity is not None
        else None
    )
    projected_error = False
    if cost is not None and capacity is not None and metrics is not None:
        # MemAvailable can be shared by many concurrent requests. Reserve each
        # in-flight footprint exactly once before it allocates; preserve the
        # existing physical 2 GiB floor.
        headroom = (max(0, metrics.memory_available_mb - PHYSICAL_MEMORY_FLOOR_MB) * _MIB
                    if metrics.memory_total_mb > 0 else 0)
        projected = ledger.projected_memory_bytes(request.owner, cost)
        if metrics.memory_total_mb <= 0 or projected > headroom:
            pressure_error = PROJECTED
            projected_error = True
        # File-backed restores spend physical headroom but no tmpfs blocks.
        # They still cannot consume physical bytes already promised to
        # RAM-backed owners above. The independent RAM projection preserves
        # their complete unswappable guarantee.
        projected_ram = ledger.projected_memory_bytes(request.owner, cost, resource="ram_backing_bytes")
        backing = metrics.memory_backing
        ram_available = (backing.available_bytes or 0) if backing is not None else (
            0 if request.node.ram_backing_configured else None)
        if ram_available is not None and projected_ram > ram_available:
            pressure_error = PROJECTED_RAM
            projected_error = True
    late = _late(request)
    if late is not None:
        # A slow collector must not grant an operation after its retry
        # deadline, even if its late sample is low.
        return late
    if pressure_error is None:
        return Grant()
    memory_wait = bool(
        cost is not None
        and cost.kind in {TransitionKind.STARTUP, TransitionKind.RESTORE}
        and metrics is not None
        and metrics.memory_total_mb > 0
        and dynamic_pressure_error(
            metrics, pressure_request, check_cpu=False, check_memory_pressure=request.check_memory_pressure,
        ) is not None
    )
    if not (projected_error or memory_wait) or request.now + RETRY_SECONDS >= request.deadline:
        return Refuse(pressure_error)
    # Keep pending demand visible until actual headroom returns.
    return Wait(pressure_error, min(RETRY_SECONDS, request.deadline - request.now))


def _late(request: AdmissionRequest) -> Refuse | None:
    if request.previous_error is not None and request.now >= request.deadline:
        return Refuse(request.previous_error)
    return None
