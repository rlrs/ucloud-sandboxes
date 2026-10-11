"""Regenerate python_goldens.json from the Python node admission.

    .venv/bin/python runtime/noded/src/admission/testdata/goldens.py

Every trace runs the Python implementation itself (docs/admission-port-3c.md,
step 3c.1):
- fair: FairCapacity under tests/test_admission_properties.FairHarness;
- ledger: TransitionLedger;
- growth_costs, growth_forecasts: DirectSandboxService's growth cost and
  forecast methods on a service built without __init__, with a fake clock;
- decide: admission_decision.gate and decide, driven as the guard drives them
  (and as tests.rs replays them), with scripted metrics. Each metrics sample
  may take time; each condition wait times out. Every case also runs
  DirectSandboxService._active_admission_guard on the same service and must
  give the same outcome.
"""
from contextlib import contextmanager
import copy
from dataclasses import replace
import json
import math
from pathlib import Path
import random
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from tests.test_admission_properties import FairHarness  # noqa: E402
from ucloud_sandboxes import direct_service  # noqa: E402
from ucloud_sandboxes.admission import FairCapacity  # noqa: E402
from ucloud_sandboxes.admission_decision import AdmissionNode, AdmissionRequest, Grant, Wait, decide, gate  # noqa: E402
from ucloud_sandboxes.direct_registry import ManagedGrowthIntent  # noqa: E402
from ucloud_sandboxes.direct_service import DirectSandboxService  # noqa: E402
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now  # noqa: E402
from ucloud_sandboxes.resident_memory import ResidentMemorySample  # noqa: E402
from ucloud_sandboxes.resource_evidence import MemoryBackingCapacity  # noqa: E402
from ucloud_sandboxes.sandbox import SandboxAdmissionClosedError, SandboxCapacityUnavailableError  # noqa: E402
from ucloud_sandboxes.transition_admission import TransitionCost, TransitionKind, TransitionLedger  # noqa: E402

MIB, GIB = 1024**2, 1024**3
KINDS = list(TransitionKind)
LIMITS = {TransitionKind.STARTUP: 2, TransitionKind.RESTORE: 1}
LIMIT_SETS = [LIMITS, {TransitionKind.STARTUP: 0, TransitionKind.RESTORE: 3}, {}]


def encode_cost(cost):
    if cost is None:
        return None
    return {"kind": cost.kind.value, "memory_bytes": cost.memory_bytes, "read_bytes": cost.read_bytes,
            "write_bytes": cost.write_bytes, "provenance": cost.provenance,
            "ram_backing_bytes": cost.ram_backing_bytes}


def encode_shape(shape):
    return None if shape is None else {"vcpu": shape.vcpu, "memory_mb": shape.memory_mb, "disk_mb": shape.disk_mb}


def encode_limits(limits):
    return [[kind.value, value] for kind, value in limits.items()]


def encode_float(value):
    return value if math.isfinite(value) else ("inf" if value > 0 else "-inf")


# --- fair --------------------------------------------------------------------

def fair_trace(rng, capacity, steps):
    trace = {"capacity": capacity, "steps": []}
    owners = ["a", "b", "c", None]
    held = []
    with FairHarness(capacity) as harness:
        for _ in range(steps):
            choice = rng.random()
            step = {}
            if choice < 0.4:
                weight = rng.randint(1, capacity + (1 if rng.random() < 0.05 else 0))
                owner, blocking = rng.choice(owners), rng.random() < 0.8
                step = {"op": "acquire", "weight": weight, "owner": owner, "blocking": blocking}
                if weight > capacity:
                    try:
                        harness.capacity.acquire(blocking=False, weight=weight, owner=owner)
                    except ValueError:
                        step["result"] = {"outcome": "invalid"}
                else:
                    outcome, ident = harness.acquire(weight, owner, blocking)
                    step["result"] = {"outcome": outcome, "id": ident}
                    if outcome == "granted":
                        held.append(weight)
            elif choice < 0.6 and (held or rng.random() < 0.2):
                if held and rng.random() < 0.9:
                    weight = held.pop(rng.randrange(len(held)))
                else:
                    weight = rng.randint(1, capacity)
                step = {"op": "release", "weight": weight}
                try:
                    harness.capacity.release(weight=weight)
                    step["result"] = "ok"
                except ValueError:
                    step["result"] = "invalid"
            elif choice < 0.7:
                owner = rng.choice(owners)
                step = {"op": "cancel", "owner": owner, "result": harness.capacity.cancel_waiters(owner)}
            elif harness.pending:
                ident = rng.choice(sorted(harness.pending))
                weight = harness.pending[ident]["weight"]
                if rng.random() < 0.75:
                    result = harness.observe(ident)
                    step = {"op": "observe", "id": ident, "result": result}
                    if result:
                        held.append(weight)
                else:
                    step = {"op": "interrupt", "id": ident, "result": harness.interrupt(ident)}
            else:
                continue
            step["state"] = harness.state()
            trace["steps"].append(step)
    return trace


# --- ledger ------------------------------------------------------------------

LEDGER_VALUES = [None, 0, 512, 1024, 4096, 2 * GIB]
LEDGER_OWNERS = [("s", index) for index in range(5)]


def random_cost(rng, values=LEDGER_VALUES, kinds=KINDS):
    return TransitionCost(rng.choice(kinds), rng.choice(values), read_bytes=rng.choice(values),
                          write_bytes=rng.choice(values), ram_backing_bytes=rng.choice(values),
                          provenance=rng.choice(["unknown", "requested-memory-bound"]))


def ledger_view(ledger):
    probes = []
    for owner in (LEDGER_OWNERS[0], ("new", 1)):
        for kind in (TransitionKind.STARTUP, TransitionKind.RESTORE):
            for value in (None, 2 * GIB):
                cost = TransitionCost(kind, value, ram_backing_bytes=value)
                probes.append({"owner": list(owner), "cost": encode_cost(cost),
                               "memory_bytes": ledger.projected_memory_bytes(owner, cost),
                               "ram_backing_bytes": ledger.projected_memory_bytes(
                                   owner, cost, resource="ram_backing_bytes")})
    return {
        "known_memory_bytes": ledger.known_memory_bytes,
        "total_ram_backing_bytes": ledger._total("ram_backing_bytes"),
        "active": {kind.value: ledger.active_count(kind) for kind in KINDS},
        "foreground_waiting": ledger.foreground_waiting,
        "projected": probes,
        "next": [{"limits": encode_limits(limits),
                  "memory_bytes": ledger.next_memory_demands(limits),
                  "ram_backing_bytes": ledger.next_memory_demands(limits, resource="ram_backing_bytes"),
                  "demand_snapshot": ledger.demand_snapshot(limits)} for limits in LIMIT_SETS],
        "snapshot": ledger.snapshot(),
        "resource_reservations": [[list(owner), encode_shape(shape)]
                                  for owner, shape in ledger.resource_reservations().items()],
    }


def ledger_trace(rng, steps):
    ledger, claims, trace = TransitionLedger(), {}, []
    waits = []
    for _ in range(steps):
        choice, owner = rng.random(), rng.choice(LEDGER_OWNERS)
        if choice < 0.2:
            cost = random_cost(rng)
            token = ledger.wait(owner, cost)
            waits.append(token)
            step = {"op": "wait", "owner": list(owner), "cost": encode_cost(cost), "result": token}
        elif choice < 0.3 and waits:
            token = rng.choice(waits)
            ledger.unwait(token)
            step = {"op": "unwait", "token": token}
        elif choice < 0.38:
            cost = random_cost(rng)
            ledger.refresh_wait_cost(owner, cost)
            step = {"op": "refresh_wait_cost", "owner": list(owner), "cost": encode_cost(cost)}
        elif choice < 0.58:
            cost = random_cost(rng)
            requested = rng.choice([None, ResourceQuantity(vcpu=0.5, memory_mb=512),
                                    ResourceQuantity(vcpu=2.0, memory_mb=4096, disk_mb=1024)])
            claim = ledger.claim(owner, cost, requested=requested)
            claims[claim.token] = claim
            step = {"op": "claim", "owner": list(owner), "cost": encode_cost(cost),
                    "requested": encode_shape(requested), "result": claim.token}
        elif choice < 0.75 and claims:
            token = rng.choice(sorted(claims))
            claim = claims[token]
            forged = rng.random() < 0.15
            if forged:
                claim = replace(claim, owner=("forged", 0))
            try:
                ledger.release(claim)
                result = "ok"
            except ValueError:
                result = "not_owned"
            step = {"op": "release", "token": token, "owner": list(claim.owner), "result": result}
        elif choice < 0.92:
            cost = None if rng.random() < 0.3 else random_cost(rng)
            ledger.set_growth_forecast(owner, cost)
            step = {"op": "set_growth_forecast", "owner": list(owner), "cost": encode_cost(cost)}
        else:
            forecasts = {rng.choice(LEDGER_OWNERS): random_cost(rng) for _ in range(rng.randint(0, 3))}
            ledger.set_growth_forecasts(forecasts)
            step = {"op": "set_growth_forecasts",
                    "forecasts": [[list(owner), encode_cost(cost)] for owner, cost in forecasts.items()]}
        step["view"] = ledger_view(ledger)
        trace.append(step)
    return trace


# --- the service, without __init__ -------------------------------------------

class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def monotonic(self):
        return self.now


class Waits:
    """_admission_changed: every wait times out, advancing the fake clock."""

    def __init__(self, clock):
        self.clock, self.log = clock, []

    def wait(self, timeout=None):
        self.log.append(timeout)
        self.clock.now += timeout
        return False

    def notify_all(self):
        pass


def service(clock, *, modes=None, default_mode="ram", samples=None, ram_root=None):
    svc = DirectSandboxService.__new__(DirectSandboxService)
    svc._capacity_guard = threading.Lock()
    svc._admission_changed = Waits(clock)
    svc._admission_open = True
    svc._active_capacity = None
    svc._runtime_metrics_provider = None
    svc._transitions = TransitionLedger()
    svc._startup_slots = FairCapacity(LIMITS[TransitionKind.STARTUP])
    svc._restore_slots = FairCapacity(LIMITS[TransitionKind.RESTORE])
    modes = {} if modes is None else modes
    svc.modes = modes
    svc.warden = SimpleNamespace(
        config=SimpleNamespace(application_memory_root=ram_root),
        application_memory_mode=lambda sandbox_id, generation: modes.get((sandbox_id, generation), default_mode))
    samples = {} if samples is None else samples
    svc.samples = samples
    svc._resident_memory = SimpleNamespace(historical=samples.get)
    svc._growth_intents = {}
    svc._provisional_growth = {}
    svc._growth_credit_expires = float("-inf")
    return svc


@contextmanager
def fake_time(clock):
    with patch.object(direct_service, "time", SimpleNamespace(monotonic=clock.monotonic)):
        yield


def make_sample(now, *, current, peak=0, shared=None, swap=0, age=0.0):
    return ResidentMemorySample(
        current_bytes=current, anonymous_bytes=0, file_bytes=current, dirty_bytes=0, writeback_bytes=0,
        refault_file_pages=0, cgroup_path="/owned", cgroup_device=1, cgroup_inode=2, sentry_pid=10,
        sentry_start_time_ticks=20, sampled_at=now - age,
        shared_memory_bytes=current if shared is None else shared, peak_bytes=peak, swap_bytes=swap)


def encode_sample(sample):
    return None if sample is None else {
        "current_bytes": sample.current_bytes, "shared_memory_bytes": sample.shared_memory_bytes,
        "peak_bytes": sample.peak_bytes, "swap_bytes": sample.swap_bytes, "sampled_at": sample.sampled_at}


# --- growth costs ------------------------------------------------------------

SAMPLE_BYTES = [0, 256 * MIB, 512 * MIB, GIB, 3 * GIB, 5 * GIB]
PEAKS = [0, GIB + 512 * MIB, 3 * GIB + 512 * MIB, 6 * GIB]
SWAPS = [0, 512 * MIB, GIB, 3 * GIB]
AGES = [0.0, 10.0, 29.9, 30.0, 30.5, 40.0]
BOUNDS = [0, GIB, 4 * GIB, 4 * GIB + 1]


def random_sample(rng, now):
    if rng.random() < 0.12:
        return None
    current = rng.choice(SAMPLE_BYTES)
    return make_sample(now, current=current, peak=rng.choice(PEAKS),
                       shared=rng.choice([None, 0, 256 * MIB, GIB]), swap=rng.choice(SWAPS),
                       age=rng.choice(AGES))


def growth_costs(rng, count):
    clock = Clock()
    cases = []
    with fake_time(clock):
        for _ in range(count):
            mode = rng.choice(["ram", "file"])
            svc = service(clock, default_mode=mode)
            owner = ("g", 7)
            sample = random_sample(rng, clock.now)
            bound = rng.choice(BOUNDS)
            kind = rng.choice([TransitionKind.STARTUP, TransitionKind.RESTORE])
            request_id = rng.choice(["", "wait-1"])
            # The explicit sample, as forecasts pass it; and the looked-up one.
            fetched = rng.random() < 0.3
            if fetched:
                svc.samples[owner] = sample
                cost = svc._growth_cost(owner, bound, kind=kind, request_id=request_id)
            else:
                cost = svc._growth_cost(owner, bound, kind=kind, request_id=request_id, sample=sample)
            cases.append({
                "mode": mode, "now": clock.now, "bound": bound, "kind": kind.value, "request_id": request_id,
                "sample": encode_sample(sample), "fetched": fetched,
                "growth_sample": svc._growth_sample(owner, sample) is not None,
                "growth_remaining": svc._growth_remaining(owner, bound, sample),
                "continuation_growth": list(svc._continuation_growth(owner, bound, sample)),
                "growth_cost": encode_cost(cost),
            })
    return cases


def startup_and_transition_costs():
    clock = Clock()
    cases = {"startup": [], "transition": []}
    with fake_time(clock):
        for mode in ("ram", "file"):
            svc = service(clock, default_mode=mode)
            for memory_mb in (0, 1, 512, 4096):
                for managed in (False, True):
                    cost = svc._startup_cost(("c", 1), ResourceQuantity(vcpu=1.0, memory_mb=memory_mb),
                                             managed=managed)
                    cases["startup"].append({"mode": mode, "memory_mb": memory_mb, "managed": managed,
                                             "cost": encode_cost(cost)})
            for kind in KINDS:
                for memory in (None, 0, GIB):
                    for ram in (None, 0, 512 * MIB):
                        for read in (None, 4096):
                            cost = svc._transition_memory_cost(kind, ("c", 1), memory, ram_backing_bytes=ram,
                                                               read_bytes=read, provenance="p")
                            cases["transition"].append({
                                "mode": mode, "kind": kind.value, "memory_bytes": memory,
                                "ram_backing_bytes": ram, "read_bytes": read, "cost": encode_cost(cost)})
    return cases


# --- growth forecasts --------------------------------------------------------

GROWTH_OWNERS = [("m", 7), ("n", 7), ("o", 3)]


def forecast_view(svc):
    ledger = svc._transitions
    return {
        "forecasts": sorted(([list(owner), encode_cost(cost)] for owner, cost in ledger._growth.items()),
                            key=lambda item: item[0]),
        "growth_totals": dict(ledger._growth_totals),
        "growth_unknown": ledger._growth_unknown,
        "credit_expires": encode_float(svc._growth_credit_expires),
    }


def growth_forecast_trace(rng, steps):
    clock = Clock()
    trace = []
    with fake_time(clock):
        default_mode = rng.choice(["ram", "file"])
        svc = service(clock, default_mode=default_mode)
        for _ in range(steps):
            choice, key = rng.random(), rng.choice(GROWTH_OWNERS)
            step = {"key": list(key)}
            with svc._capacity_guard:
                if choice < 0.22:
                    phase = rng.choice(["queued", "active", "active", "safe", "parked", "terminal"])
                    intent = ManagedGrowthIntent(key[0], key[1], rng.choice(["", "job"]), "",
                                                 rng.choice(BOUNDS), phase, rng.choice(["", "", "wait-1"]))
                    svc._growth_intents[key] = intent
                    svc._update_growth_forecast_locked(key)
                    step.update(op="intent", intent={
                        "job_id": intent.job_id, "memory_bytes": intent.memory_bytes,
                        "phase": intent.phase, "request_id": intent.request_id})
                elif choice < 0.3:
                    svc._growth_intents.pop(key, None)
                    svc._provisional_growth.pop(key, None)
                    svc._update_growth_forecast_locked(key)
                    step.update(op="forget")
                elif choice < 0.38:
                    cost = random_cost(rng, [None, 0, GIB, 4 * GIB],
                                       [TransitionKind.STARTUP, TransitionKind.RESTORE])
                    svc._provisional_growth[key] = cost
                    svc._update_growth_forecast_locked(key)
                    step.update(op="provisional", cost=encode_cost(cost))
                elif choice < 0.44:
                    svc._provisional_growth.pop(key, None)
                    svc._update_growth_forecast_locked(key)
                    step.update(op="clear_provisional")
                elif choice < 0.62:
                    sample = random_sample(rng, clock.now)
                    if sample is None:
                        svc.samples.pop(key, None)
                    else:
                        svc.samples[key] = sample
                    step.update(op="sample", sample=encode_sample(sample))
                elif choice < 0.68:
                    mode = rng.choice(["ram", "file"])
                    svc.modes[key] = mode
                    step.update(op="mode", mode=mode)
                elif choice < 0.78:
                    seconds = rng.choice([0.1, 1.0, 5.0, 20.0, 31.0])
                    clock.now += seconds
                    step.update(op="advance", seconds=seconds)
                elif choice < 0.86:
                    svc._refresh_growth_forecasts_locked()
                    step.update(op="refresh")
                else:
                    demand = svc._next_memory_demand_locked()
                    step.update(op="demand", result=[demand.physical_bytes, demand.ram_backing_bytes])
            step["now"] = clock.now
            step["view"] = forecast_view(svc)
            trace.append(step)
    return {"limits": encode_limits(LIMITS), "default_mode": default_mode, "steps": trace}


def managed_growth_cases():
    """tests/test_managed_growth.py:319-427, at the forecast level: the
    intent each service call commits, then what warm_park_demand bills."""
    clock = Clock()
    cases = []
    with fake_time(clock):
        def case(name, mode, steps):
            svc = service(clock, default_mode=mode)
            out = []
            for step in steps:
                op = step["op"]
                with svc._capacity_guard:
                    if op == "intent":
                        key = tuple(step["key"])
                        svc._growth_intents[key] = ManagedGrowthIntent(
                            key[0], key[1], step.get("job_id", "job"), "", step["memory_bytes"],
                            step["phase"], step.get("request_id", ""))
                        svc._update_growth_forecast_locked(key)
                    elif op == "observe":
                        key = tuple(step["key"])
                        s = step["sample"]
                        svc.samples[key] = make_sample(clock.now, current=s["current"], peak=s.get("peak", 0),
                                                       shared=s.get("shared"), swap=s.get("swap", 0),
                                                       age=s.get("age", 0.0))
                        svc._refresh_growth_forecasts_locked()
                    elif op == "advance":
                        clock.now += step["seconds"]
                    elif op == "demand":
                        demand = svc._next_memory_demand_locked()
                        step = dict(step, result=[demand.physical_bytes, demand.ram_backing_bytes],
                                    unknown=svc._transitions.demand_snapshot(LIMITS)[
                                        "unknown_transition_memory_costs"])
                out.append(dict(step, now=clock.now))
            cases.append({"name": name, "mode": mode, "steps": out})

        bound = 4 << 30
        launch = {"op": "intent", "key": ["one", 7], "memory_bytes": bound, "phase": "active"}
        continuation = dict(launch, request_id="wait-1")
        demand = {"op": "demand"}
        case("residual_forecast_credits_only_fresh_incarnation_footprint", "ram", [
            launch,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 3 << 30, "shared": 2 << 30}}, demand,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 3 << 30, "shared": 2 << 30, "age": 1000.0}},
            demand,
            {"op": "observe", "key": ["one", 8], "sample": {"current": 3 << 30, "shared": 2 << 30}}, demand,
        ])
        case("a_swapped_continuation_owes_its_prefetch_not_its_whole_swap", "ram", [
            continuation,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 1 << 29, "peak": 7 << 29, "swap": 3 << 30}},
            demand,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 1 << 29, "peak": 7 << 29}}, demand,
        ])
        case("continuation_forecasts_physical_growth_to_its_demonstrated_peak", "ram", [
            continuation,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 1 << 30, "peak": 3 << 29}}, demand,
            {"op": "intent", "key": ["two", 7], "memory_bytes": bound, "phase": "active"},
            {"op": "observe", "key": ["two", 7], "sample": {"current": 1 << 30, "peak": 3 << 29}}, demand,
        ])
        case("growth_credit_survives_a_slow_refresh_pass", "file", [
            launch,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 3 << 30, "age": 10.0}}, demand,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 3 << 30, "age": 40.0}}, demand,
            {"op": "observe", "key": ["one", 7], "sample": {"current": 3 << 30, "age": 29.9}}, demand,
            {"op": "advance", "seconds": 0.2}, demand,
        ])
        case("terminal_releases_only_the_terminal_primary", "file", [
            launch, demand,
            dict(launch, phase="terminal"), demand,
        ])
        case("file_growth_does_not_reappear_when_reclaimable_pages_are_evicted", "file", [
            dict(continuation, request_id="file-wait"),
            *[item for resident in (3 << 30, 64 << 20, 0) for item in (
                {"op": "observe", "key": ["one", 7], "sample": {"current": resident, "shared": 0}}, demand)],
        ])
    return cases


# --- decide ------------------------------------------------------------------

DECIDE_VALUES = [None, 0, 512 * MIB, GIB, GIB + 1, 2 * GIB, 4 * GIB]
DECIDE_OWNERS = [("d", index) for index in range(4)]


def random_metrics(rng):
    if rng.random() < 0.06:
        return None
    total = rng.choice([0, 16384, 65536, 65536, 65536])
    backing = rng.choice([None, None, MemoryBackingCapacity(),
                          MemoryBackingCapacity(32 * GIB, rng.choice([0, GIB, 4 * GIB, 16 * GIB, 32 * GIB]), "1:2:3"),
                          MemoryBackingCapacity(32 * GIB, 32 * GIB, "1:2:3")])
    return NodeRuntimeMetrics(
        collected_at=utc_now(), memory_total_mb=total,
        memory_available_mb=rng.choice([0, 1024, 2047, 2048, 3072, 4096, 6144, 20000, 40000, 60000, 60000]),
        swap_total_mb=rng.choice([0, 0, 8192]), swap_free_mb=rng.choice([0, 4096]),
        memory_psi_full_avg10=rng.choice([None, 0.0, 0.0, 0.0, 9.99, 10.0, 50.0]), memory_backing=backing)


def encode_metrics(metrics):
    if metrics is None:
        return None
    backing = metrics.memory_backing
    return {"memory_total_mb": metrics.memory_total_mb, "memory_available_mb": metrics.memory_available_mb,
            "swap_total_mb": metrics.swap_total_mb, "swap_free_mb": metrics.swap_free_mb,
            "memory_psi_full_avg10": metrics.memory_psi_full_avg10,
            "memory_backing": None if backing is None else {
                "total_bytes": backing.total_bytes, "available_bytes": backing.available_bytes,
                "identity": backing.identity}}


def encode_outcome(decision, waits, end):
    if isinstance(decision, Grant):
        outcome = {"grant": True}
    else:
        outcome = {"refuse": "closed" if decision.closed else "capacity", "error": decision.reason}
    return {**outcome, "waits": waits, "end": end}


def drive_decide(ledger, request, steps, requotes):
    """The guard's loop around gate and decide, as tests.rs replays it."""
    now, attempt, waits = request.now, 0, []
    while True:
        request = replace(request, now=now)
        decision = gate(request)
        if decision is not None:
            break
        step = steps[min(attempt, len(steps) - 1)]
        attempt += 1
        metrics = None
        if request.node.capacity is not None:
            now += step["sample_seconds"]
            metrics = step["metrics"]
        if requotes:
            request = replace(request, cost=step["cost"])
        request = replace(request, now=now)
        if requotes and request.node.capacity is not None and metrics is not None:
            ledger.refresh_wait_cost(request.owner, request.cost)
        decision = decide(metrics, ledger, request)
        if not isinstance(decision, Wait):
            break
        waits.append(decision.seconds)
        request = replace(request, previous_error=decision.reason)
        now += decision.seconds
    return encode_outcome(decision, waits, now)


def drive_guard(svc, clock, request, steps, requotes):
    """The same case through DirectSandboxService._active_admission_guard."""
    attempt = {"index": 0}

    def provider():
        step = steps[min(attempt["index"], len(steps) - 1)]
        clock.now += step["sample_seconds"]
        return step["metrics"]

    def cost_provider():
        step = steps[min(attempt["index"], len(steps) - 1)]
        attempt["index"] += 1
        return step["cost"]

    def advance():
        attempt["index"] += 1

    svc._runtime_metrics_provider = provider
    if requotes:
        kwargs = {"transition_cost_provider": cost_provider}
    else:
        # Without a requote, the next sample comes from the next step.
        kwargs = {"validate_owner": advance}
    log = svc._admission_changed.log
    with fake_time(clock):
        try:
            with svc._active_admission_guard(
                    request.requested, check_shape=True, check_memory_pressure=request.check_memory_pressure,
                    transition_cost=request.cost, transition_owner=request.owner, deadline=request.deadline,
                    **kwargs):
                outcome = {"grant": True}
        except SandboxAdmissionClosedError as exc:
            outcome = {"refuse": "closed", "error": str(exc)}
        except SandboxCapacityUnavailableError as exc:
            outcome = {"refuse": "capacity", "error": str(exc)}
    return {**outcome, "waits": list(log), "end": clock.now}


def decide_case(rng):
    clock = Clock()
    ram_root = rng.choice([None, "/ram"])
    svc = service(clock, ram_root=ram_root)
    # The prefix's forecasts stand as given: no intents to refresh them from
    # (the driver refreshes aged forecasts before deciding).
    svc._growth_credit_expires = float("inf")
    svc._admission_open = rng.random() < 0.95
    capacity = rng.choice([None, *[ResourceQuantity(vcpu=8.0, memory_mb=65536)] * 7,
                           ResourceQuantity(vcpu=1.0, memory_mb=2048)])
    svc._active_capacity = capacity
    owner = rng.choice(DECIDE_OWNERS)
    prefix = []
    for _ in range(rng.randint(0, 6)):
        other, cost = rng.choice(DECIDE_OWNERS), random_cost(rng, DECIDE_VALUES)
        what = rng.choice(["claim", "wait", "forecast"])
        if what == "claim":
            svc._transitions.claim(other, cost)
        elif what == "wait":
            svc._transitions.wait(other, cost)
        else:
            svc._transitions.set_growth_forecast(other, cost)
        prefix.append({"op": what, "owner": list(other), "cost": encode_cost(cost)})
    requested = rng.choice([*[ResourceQuantity(vcpu=1.0, memory_mb=1024)] * 3, ResourceQuantity(vcpu=0.0, memory_mb=4096),
                            ResourceQuantity(vcpu=16.0, memory_mb=512), ResourceQuantity(vcpu=0.0, memory_mb=0),
                            ResourceQuantity(vcpu=1.0, memory_mb=4096), ResourceQuantity(vcpu=0.0, memory_mb=1500)])
    cost = None if rng.random() < 0.1 else random_cost(rng, DECIDE_VALUES)
    if cost is not None and rng.random() < 0.7:
        # Callers publish their demand before the guard (_transition_demand).
        svc._transitions.wait(owner, cost)
        prefix.append({"op": "wait", "owner": list(owner), "cost": encode_cost(cost)})
    requotes = cost is not None and cost.kind in {TransitionKind.STARTUP, TransitionKind.RESTORE} and rng.random() < 0.25
    steps = []
    for _ in range(rng.randint(1, 6)):
        step = {"metrics": random_metrics(rng), "sample_seconds": rng.choice([0.0, 0.0, 0.05, 0.3, 2.0])}
        if requotes:
            step["cost"] = TransitionCost(cost.kind, rng.choice(DECIDE_VALUES),
                                          ram_backing_bytes=rng.choice(DECIDE_VALUES), provenance="requote")
        steps.append(step)
    check_memory_pressure = rng.random() < 0.85
    deadline_seconds = rng.choice([0.0, 0.1, 0.25, 0.5, 1.0, 1.0, 30.0, 30.0])
    start = clock.now
    ledger = copy.deepcopy(svc._transitions)
    request = AdmissionRequest(
        node=AdmissionNode(svc._admission_open, capacity, ram_backing_configured=ram_root is not None), owner=owner,
        requested=requested, cost=cost, check_shape=True, check_memory_pressure=check_memory_pressure,
        deadline=start + deadline_seconds, now=start)
    outcome = drive_decide(ledger, request, steps, requotes)
    guarded = drive_guard(svc, clock, request, steps, requotes)
    assert guarded == outcome, (outcome, guarded)
    return {
        "admission_open": svc._admission_open, "capacity": encode_shape(capacity),
        "ram_backing_configured": ram_root is not None, "owner": list(owner), "requested": encode_shape(requested),
        "cost": encode_cost(cost), "check_memory_pressure": check_memory_pressure, "start": start,
        "deadline": start + deadline_seconds, "prefix": prefix,
        "steps": [{"metrics": encode_metrics(step["metrics"]), "sample_seconds": step["sample_seconds"],
                   "cost": encode_cost(step.get("cost"))} for step in steps],
        "outcome": outcome,
    }


def main():
    rng = random.Random(3)
    golden = {
        "fair": [fair_trace(rng, capacity, 40) for capacity in (1, 1, 4, 4, 6, 6, 6, 10)],
        "ledger": [ledger_trace(rng, 50) for _ in range(8)],
        "growth_costs": growth_costs(rng, 600),
        "costs": startup_and_transition_costs(),
        "growth_forecasts": [growth_forecast_trace(rng, 50) for _ in range(12)],
        "managed_growth": managed_growth_cases(),
        "decide": [decide_case(rng) for _ in range(800)],
    }
    out = Path(__file__).with_name("python_goldens.json")
    out.write_text(json.dumps(golden, indent=None, sort_keys=True, separators=(",", ":")) + "\n")


main()
