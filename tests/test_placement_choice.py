"""Power-of-k placement library (plan C4.3) and its herding simulation.

The simulation gives the library and the gateway's lexicographic ranking the
same information, which is C4.3's regime: one stale fleet view plus each
process's own overlay. Run with PLACEMENT_SIMULATION_REPORT=1 to print the
table, with a reference row for today's serialized shared-route view.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import timedelta
import heapq
import os
import random
from statistics import median
import unittest

from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.models import (
    SOFT_DRAIN_LABEL,
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    is_soft_drained,
    utc_now,
)
from ucloud_sandboxes.placement_accounting import (
    PlacementReservation,
    _node_available_resources,
)
from ucloud_sandboxes.placement_choice import (
    PENDING_RESIDENCY,
    RESIDENCY_WEIGHT,
    FleetView,
    GroupSlice,
    InflightOverlay,
    PlacementRequest,
    PowerOfKChooser,
    SandboxIncarnation,
    fit_count,
    initial_charge,
    node_fits,
)
from ucloud_sandboxes.resource_admission import node_pressure_score

NOW = utc_now()
SHAPE = ResourceQuantity(1, 2048, 8192)
SPEC_HASH = "a" * 64
PLAIN = PlacementRequest(SHAPE)
IMG = PlacementRequest(SHAPE, image="img")
TOTAL_MEMORY_MB = 262_144


def heartbeat(node, *, slots=64, used=0, creating=0, cpu=10.0, inventory=(),
              epoch="epoch-1", **changes):
    """A direct worker whose disk fits ``slots`` sandboxes of SHAPE."""

    return replace(NodeHeartbeat(
        node_id=node, job_id=f"job-{node}", updated_at=NOW, received_at=NOW,
        active_sandboxes=used, active_sandbox_creates=creating,
        node_url=f"http://{node}:8090", agent_version=package_version(),
        capabilities=("sandbox", "disk-quota"),
        total_resources=ResourceQuantity(32, TOTAL_MEMORY_MB, slots * SHAPE.disk_mb),
        resources_known=True,
        used_resources=ResourceQuantity(
            used * SHAPE.vcpu, used * SHAPE.memory_mb, used * SHAPE.disk_mb,
        ),
        runtime_metrics=NodeRuntimeMetrics(
            collected_at=NOW, cpu_percent=cpu, memory_percent=5.0,
            memory_total_mb=TOTAL_MEMORY_MB, memory_available_mb=200_000,
        ),
        node_epoch=epoch, inventory=tuple(inventory),
    ), **changes)


def incarnation(index, generation=1):
    return SandboxIncarnation(f"sbx-{index}", generation, SPEC_HASH, f"op-{index}")


def observed(item: SandboxIncarnation) -> SandboxInventoryEntry:
    return SandboxInventoryEntry(
        sandbox_id=item.sandbox_id, generation=item.generation,
        operation_id=item.operation_id, spec_hash=item.spec_hash,
        state="running", resources=SHAPE,
    )


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FleetViewTests(unittest.TestCase):
    def test_ready_keeps_fresh_admitting_current_sandbox_workers(self):
        good = heartbeat("good")
        view = FleetView.ready([
            good,
            heartbeat("stale", received_at=NOW - timedelta(seconds=121)),
            heartbeat("draining", draining=True),
            heartbeat("closed", admission_open=False),
            heartbeat("no-url", node_url=None),
            heartbeat("builder", capabilities=("image-build",)),
            heartbeat("old-agent", agent_version="0.0.0-old"),
        ], now=NOW, ttl_seconds=120)
        self.assertEqual(view.nodes, (good,))
        self.assertEqual(view.routes.routes_for(good), [])


class FitTests(unittest.TestCase):
    def test_fit_charges_records_in_placement_accounting_terms(self):
        node = heartbeat("a", slots=4, used=2)
        charges = [
            PlacementReservation(f"r{index}", "a", "job-a", "http://a:8090", SHAPE, "")
            for index in range(2)
        ]
        self.assertTrue(node_fits(node, SHAPE, charges[:1]))
        self.assertFalse(node_fits(node, SHAPE, charges))
        self.assertEqual(_node_available_resources(node, charges).disk_mb, 0)
        self.assertEqual(fit_count(node, PLAIN, [], 10), 2)
        self.assertEqual(fit_count(node, PLAIN, charges[:1], 10), 1)
        self.assertEqual(fit_count(heartbeat("b"), PLAIN, [], 5), 5)

    def test_storage_devices_bound_fit_and_cpu_only_ranks(self):
        native = heartbeat(
            "a", slots=64, cpu=99.0,
            capabilities=("sandbox", "disk-quota", "storage-native-v1"),
        )
        native = replace(native, runtime_metrics=replace(
            native.runtime_metrics, storage_hard_capacity_mb=64 * SHAPE.disk_mb,
            storage_ublk_max_devices=4, storage_ublk_active_devices=1,
        ))
        self.assertEqual(fit_count(native, PLAIN, [], 10), 3)
        self.assertEqual(fit_count(heartbeat("b", cpu=99.0), PLAIN, [], 3), 3)

    def test_dynamic_claim_workers_charge_the_initial_claim(self):
        node = heartbeat("a", slots=4)
        node = replace(node, runtime_metrics=replace(
            node.runtime_metrics, storage_workspace_grant_mb=1024,
            storage_memory_idle_claim_mb=512,
        ))
        parkable = PlacementRequest(
            SHAPE, spec={"parkable": True, "disk_mb": 4096, "memory_mb": 2048},
        )
        self.assertEqual(initial_charge(node, parkable).disk_mb, 1024 + 512)
        self.assertEqual(initial_charge(node, PLAIN), SHAPE)
        # Each create must fit its whole disk but charges only its claim.
        self.assertEqual(fit_count(node, parkable, [], 64), 17)
        self.assertEqual(fit_count(node, PLAIN, [], 64), 4)
        overlay = InflightOverlay(clock=FakeClock())
        overlay.reserve(node, incarnation(1), parkable)
        (record,) = overlay.records(node)
        self.assertEqual(record.resources, replace(SHAPE, disk_mb=1536))


class OverlayTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.overlay = InflightOverlay(ttl_seconds=30, clock=self.clock)
        self.node = heartbeat("a")
        self.overlay.reserve(self.node, incarnation(1), IMG)

    def test_charged_until_the_exact_incarnation_is_reported(self):
        stale = replace(self.node, inventory=(
            observed(incarnation(1, generation=2)),
            observed(SandboxIncarnation("sbx-1", 1, SPEC_HASH, "op-other")),
        ))
        (record,) = self.overlay.records(stale)
        self.assertEqual((record.job_id, record.resources, record.image), ("job-a", SHAPE, "img"))
        self.assertEqual(self.overlay.records(heartbeat("b")), ())
        confirmed = replace(self.node, inventory=(observed(incarnation(1)),))
        self.assertEqual(self.overlay.records(confirmed), ())
        self.assertEqual(self.overlay.records(self.node), ())

    def test_a_retired_worker_incarnation_cannot_receive_the_create(self):
        # A view that merely differs (an older one, say) keeps the charge.
        self.assertEqual(len(self.overlay.records(replace(self.node, node_epoch="epoch-0"))), 1)
        rebooted = replace(self.node, node_epoch="epoch-2", retired_node_epochs=("epoch-1",))
        self.assertEqual(self.overlay.records(rebooted), ())
        self.assertEqual(self.overlay.records(self.node), ())

    def test_ttl_bounds_a_lost_release(self):
        self.clock.now = 29.9
        self.assertEqual(len(self.overlay.records(self.node)), 1)
        self.clock.now = 30.0
        self.assertEqual(self.overlay.records(self.node), ())

    def test_reservations_on_a_worker_that_left_the_view_expire(self):
        self.clock.now = 20.0
        self.overlay.reserve(heartbeat("b"), incarnation(2), PLAIN)
        self.assertEqual(self.overlay.reservation_count(), 2)
        self.clock.now = 30.0
        self.overlay.reserve(heartbeat("b"), incarnation(3), PLAIN)
        self.assertEqual(self.overlay.reservation_count(), 2)
        self.clock.now = 60.0
        self.overlay.reserve(heartbeat("b"), incarnation(4), PLAIN)
        self.assertEqual(self.overlay.reservation_count(), 1)

    def test_each_heartbeat_settles_once(self):
        replayed = replace(self.node, inventory=(observed(incarnation(2)),))
        self.assertEqual(len(self.overlay.records(replayed)), 1)
        # A replayed create the worker already holds is overcharged until the
        # next heartbeat object, never undercharged.
        self.overlay.reserve(replayed, incarnation(2), PLAIN)
        self.assertEqual(len(self.overlay.records(replayed)), 2)
        self.assertEqual(len(self.overlay.records(replace(replayed))), 1)

    def test_reserve_is_idempotent_and_release_is_exact(self):
        self.overlay.reserve(self.node, incarnation(1), IMG)
        self.overlay.reserve(self.node, incarnation(2), PLAIN)
        self.assertEqual(len(self.overlay.records(self.node)), 2)
        self.overlay.release("job-b", incarnation(2))
        self.overlay.release("job-a", incarnation(2))
        self.overlay.release("job-a", incarnation(2))
        self.assertEqual(len(self.overlay.records(self.node)), 1)


def chooser(*, k=3, seed=1, overlay=None, api_processes=1):
    if overlay is None:
        overlay = InflightOverlay(clock=FakeClock())
    return PowerOfKChooser(
        overlay, rng=random.Random(seed), target_creates_per_node=8,
        api_processes=api_processes, k=k,
    )


def node_ids(nodes):
    return [node.node_id for node in nodes]


class ChooserTests(unittest.TestCase):
    def test_samples_k_eligible_uniformly_and_orders_best_first(self):
        nodes = FleetView(tuple(heartbeat(f"n{index}", cpu=10.0 + index) for index in range(10)))
        sampler, counts = chooser(), Counter()
        for _ in range(3000):
            ranked = sampler.choose(nodes, PlacementRequest(SHAPE))
            self.assertEqual(len(set(node_ids(ranked))), 3)
            scores = [node_pressure_score(node) for node in ranked]
            self.assertEqual(scores, sorted(scores))
            counts.update(node_ids(ranked))
        for node in nodes.nodes:
            self.assertAlmostEqual(counts[node.node_id] / 3000, 0.3, delta=0.04)
        self.assertEqual(
            node_ids(chooser(seed=7).choose(nodes, PlacementRequest(SHAPE))),
            node_ids(chooser(seed=7).choose(nodes, PlacementRequest(SHAPE))),
        )

    def test_filters_exclusions_capabilities_and_fit(self):
        view = FleetView((
            heartbeat("excluded"), heartbeat("full", slots=2, used=2), heartbeat("plain"),
            heartbeat("gpu", capabilities=("sandbox", "disk-quota", "gpu")),
        ))
        request = PlacementRequest(
            SHAPE, capabilities=("gpu",), excluded_job_ids=frozenset({"job-excluded"}),
        )
        self.assertEqual(node_ids(chooser().choose(view, request)), ["gpu"])
        self.assertEqual(
            set(node_ids(chooser(k=4).choose(view, replace(request, capabilities=())))),
            {"plain", "gpu"},
        )

    def test_durable_view_reservations_count_against_fit(self):
        node = heartbeat("a", slots=2, used=1)
        migration = PlacementReservation("m1", "a", "job-a", "http://a:8090", SHAPE, "")
        view = FleetView.ready([node], now=NOW, ttl_seconds=120, routes=[migration])
        self.assertEqual(chooser().choose(view, PlacementRequest(SHAPE)), ())

    def test_soft_drained_workers_are_a_last_resort(self):
        drained = heartbeat("drained", labels={SOFT_DRAIN_LABEL: NOW.isoformat()})
        busy = heartbeat("busy", cpu=95.0)
        request = PlacementRequest(SHAPE)
        self.assertEqual(node_ids(chooser(k=2).choose(FleetView((drained, busy)), request)),
                         ["busy", "drained"])
        self.assertEqual(node_ids(chooser(k=1).choose(FleetView((drained, busy)), request)), ["busy"])
        self.assertEqual(node_ids(chooser(k=1).choose(FleetView((drained,)), request)), ["drained"])

    def test_own_creates_and_residency_shift_the_score(self):
        overlay = InflightOverlay(clock=FakeClock())
        sampler = chooser(k=2, overlay=overlay)
        warm, cold = heartbeat("warm"), heartbeat("cold")
        view = FleetView((warm, cold))
        request = PlacementRequest(SHAPE, image="img", residency={"warm": 1.0})
        # Each own create adds 1/8 load and the resident image is worth 1/4,
        # so the warm worker leads until its third create.
        self.assertEqual(RESIDENCY_WEIGHT, 0.25)
        for index in range(3):
            if index < 2:
                self.assertEqual(sampler.choose(view, request)[0].node_id, "warm")
            overlay.reserve(warm, incarnation(index), IMG)
        self.assertEqual(sampler.choose(view, request)[0].node_id, "cold")
        # An image this process is already sending counts as partly resident.
        self.assertEqual(PENDING_RESIDENCY, 0.5)
        overlay.reserve(cold, incarnation(9), IMG)
        # cold: 0.1 + 1/8 - 1/8 for img, 0.1 + 1/8 otherwise; other: 0.15.
        other = heartbeat("other", cpu=15.0)
        ranked = chooser(k=2, overlay=overlay).choose(
            FleetView((cold, other)), PlacementRequest(SHAPE, image="img"),
        )
        self.assertEqual(node_ids(ranked), ["cold", "other"])
        ranked = chooser(k=2, overlay=overlay).choose(
            FleetView((cold, other)), PlacementRequest(SHAPE, image="another"),
        )
        self.assertEqual(node_ids(ranked), ["other", "cold"])

    def test_own_creates_scale_by_api_processes_but_fit_stays_exact(self):
        overlay = InflightOverlay(clock=FakeClock())
        mine, reported = heartbeat("mine", slots=2), heartbeat("reported", creating=2)
        overlay.reserve(mine, incarnation(1), PLAIN)
        view, request = FleetView((mine, reported)), PlacementRequest(SHAPE)
        # One own create among N processes estimates N fleet creates.
        self.assertEqual(node_ids(chooser(k=2, overlay=overlay).choose(view, request)),
                         ["mine", "reported"])
        self.assertEqual(
            node_ids(chooser(k=2, overlay=overlay, api_processes=4).choose(view, request)),
            ["reported", "mine"],
        )
        crowded = chooser(k=2, overlay=overlay, api_processes=64).choose(view, request)
        self.assertEqual(node_ids(crowded), ["reported", "mine"])

    def test_invalid_parameters_are_rejected(self):
        for arguments in ({"k": 0}, {"api_processes": 0}):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                chooser(**arguments)
        with self.assertRaises(ValueError):
            PowerOfKChooser(InflightOverlay(), rng=random.Random(),
                            target_creates_per_node=0, api_processes=1)
        with self.assertRaises(ValueError):
            chooser().pack(FleetView(()), PlacementRequest(SHAPE), 1, per_node_budget=0)
        for ttl in (0, -1.0, float("nan")):
            with self.subTest(ttl=ttl), self.assertRaises(ValueError):
                InflightOverlay(ttl_seconds=ttl)


class GroupPackTests(unittest.TestCase):
    def test_fills_one_worker_to_the_budget_then_overflows(self):
        view = FleetView(tuple(heartbeat(f"n{index}") for index in range(4)))
        request = PlacementRequest(SHAPE, image="img", residency={"n2": 1.0})
        slices = chooser(k=4).pack(view, request, 20, per_node_budget=8)
        self.assertEqual([item.count for item in slices], [8, 8, 4])
        self.assertEqual(slices[0].heartbeat.node_id, "n2")
        self.assertEqual(len({item.heartbeat.job_id for item in slices}), 3)

    def test_fit_and_overlay_bound_each_slice_and_shortfall_is_returned(self):
        overlay = InflightOverlay(clock=FakeClock())
        small, smaller = heartbeat("small", slots=5, used=1), heartbeat("smaller", slots=3)
        overlay.reserve(small, incarnation(1), PLAIN)
        slices = chooser(overlay=overlay).pack(
            FleetView((small, smaller)), PlacementRequest(SHAPE), 10, per_node_budget=8,
        )
        self.assertEqual(sorted((item.heartbeat.node_id, item.count) for item in slices),
                         [("small", 3), ("smaller", 3)])
        self.assertIsInstance(slices[0], GroupSlice)
        self.assertEqual(chooser().pack(FleetView(()), PLAIN, 2, per_node_budget=2), ())
        excluded = PlacementRequest(SHAPE, excluded_job_ids=frozenset({"job-small"}))
        self.assertEqual(
            [(item.heartbeat.node_id, item.count) for item in chooser().pack(
                FleetView((small, smaller)), excluded, 2, per_node_budget=8)],
            [("smaller", 2)],
        )


# Simulation -----------------------------------------------------------------


class LexicographicRanking:
    """The essence of gateway Placement.rank (lexicographic min
    over every candidate) given the same inputs as power-of-k: the fleet view
    plus this process's own unconfirmed creates, which play the part of
    InflightCreatePlacements claims and of its own committed creating routes.
    Heartbeat used resources stand in for durably assigned shapes. Rejected
    workers are excluded and the rest re-ranked, as Placement.select_and_reserve
    does, which here is the remaining order."""

    def __init__(self, overlay, rng, target, processes):
        self.overlay, self.target = overlay, target

    def choose(self, fleet, request):
        candidates = []
        for node in fleet.nodes:
            own = list(self.overlay.records(node))
            if node.job_id not in request.excluded_job_ids and node_fits(node, request.resources, own):
                candidates.append((self.rank(node, own, request), node))
        return tuple(node for _rank, node in sorted(candidates, key=lambda item: item[0]))

    def rank(self, node, own, request):
        total = node.total_resources
        shape = max(
            (node.used_resources.vcpu + sum(r.resources.vcpu for r in own)) / max(1, total.vcpu),
            (node.used_resources.memory_mb + sum(r.resources.memory_mb for r in own))
            / max(1, total.memory_mb),
        )
        active_creates = node.active_sandbox_creates + len(own)
        load = node_pressure_score(node) + active_creates / self.target
        busy = load >= 0.6  # gateway.placement._AFFINITY_LOAD_BAND
        cached = request.residency.get(node.node_id, 0.0) >= 1.0
        sending = not cached and any(r.image == request.image for r in own)
        free = _node_available_resources(node, own)
        return (
            is_soft_drained(node), busy,
            shape if busy else 0.0, load if busy else 0.0,
            0 if cached else 1 if sending else 2,
            # No registry manifest: _cold_image_placement_cost_for_state is
            # (1, max(in-flight image count, active creates)).
            (1, max(int(sending), active_creates)),
            shape, load, active_creates,
            (max(0.0, free.vcpu - request.resources.vcpu),
             max(0, free.memory_mb - request.resources.memory_mb),
             max(0, free.disk_mb - request.resources.disk_mb)),
            node.node_id,
        )


class LexicographicFidelityTests(unittest.TestCase):
    def test_reimplementation_picks_what_rank_candidates_picks(self):
        # Fails rather than skips when the live ranking moves (C6.1) or goes
        # (C4.3 wiring): the simulation's baseline must not drift unchecked.
        from tests.gateway_support import gateway_services
        from ucloud_sandboxes.gateway.placement import NodePlacementState

        placement = gateway_services(create_target_concurrency_per_node=8).placement
        rng, contested = random.Random(42), 0
        for trial in range(400):
            overlay, nodes = InflightOverlay(clock=FakeClock()), []
            for index in range(rng.randint(1, 8)):
                nodes.append(heartbeat(
                    f"n{index}", slots=rng.choice([8, 32, 64]), used=rng.randint(0, 8),
                    creating=rng.randint(0, 12), cpu=rng.choice([5.0, 20.0, 45.0, 70.0, 95.0]),
                    labels={SOFT_DRAIN_LABEL: "x"} if rng.random() < 0.1 else {},
                ))
                for claim in range(rng.randint(0, 4)):
                    overlay.reserve(nodes[-1], incarnation(f"{trial}-{index}-{claim}"),
                                    rng.choice([IMG, PLAIN]))
            request = replace(IMG, residency={n.node_id: 1.0 for n in nodes if rng.random() < 0.3})
            ranked = LexicographicRanking(overlay, None, 8, 1).choose(FleetView(tuple(nodes)), request)
            states, sending = [], set()
            for node in nodes:
                own = list(overlay.records(node))
                if not node_fits(node, SHAPE, own):
                    continue
                vcpu = node.used_resources.vcpu + sum(r.resources.vcpu for r in own)
                memory = node.used_resources.memory_mb + sum(r.resources.memory_mb for r in own)
                pending = node.node_id not in request.residency and any(r.image == "img" for r in own)
                sending |= {node.node_id} if pending else set()
                states.append((node, NodePlacementState(
                    available_resources=_node_available_resources(node, own),
                    inflight_image_identities=frozenset({"img"} if pending else ()),
                    projected_image_identities=frozenset(),
                    active_creates=node.active_sandbox_creates + len(own),
                    assigned_shape_pressure=max(vcpu / 32, memory / TOTAL_MEMORY_MB),
                    assigned_vcpu=vcpu, assigned_memory_mb=memory,
                )))
            if not states:
                self.assertEqual(ranked, ())
                continue
            chosen = placement.rank(states, SHAPE, "img", set(request.residency), sending, None)
            self.assertEqual(chosen.node_id, ranked[0].node_id)
            contested += len(states) > 1
        self.assertGreater(contested, 200)


def power_of_k(overlay, rng, target, processes):
    return PowerOfKChooser(
        overlay, rng=rng, target_creates_per_node=target, api_processes=processes,
    )


@dataclass
class SimWorker:
    node: str
    slots: int
    base_cpu: float
    accepted: list = field(default_factory=list)
    finishes: list = field(default_factory=list)
    rejected: int = 0
    published: NodeHeartbeat | None = None

    def creating(self, now):
        while self.finishes and self.finishes[0] <= now:
            heapq.heappop(self.finishes)
        return len(self.finishes)

    def publish(self, now):
        creating = self.creating(now)
        self.published = heartbeat(
            self.node, slots=self.slots, used=len(self.accepted), creating=creating,
            # Starting sandboxes cost CPU, so a burst shows only in the next beat.
            cpu=min(100.0, self.base_cpu + 2.0 * creating),
            inventory=[observed(item) for item in self.accepted],
        )


@dataclass(frozen=True)
class Scenario:
    name: str
    nodes: int = 10
    processes: int = 8
    creates: int = 600
    seconds: float = 2.0
    slots: int = 192
    # Workers that already run sandboxes, at higher CPU, before the burst.
    loaded_nodes: int = 0
    loaded_sandboxes: int = 0
    resident_nodes: int = 0
    heartbeat_seconds: float = 2.0
    startup_seconds: tuple[float, float] = (0.5, 1.5)
    target: int = 8


def simulate(policy, scenario: Scenario, seed: int, *, shared_overlay=False) -> dict:
    """Place a create burst from independent API processes. Each worker
    publishes a heartbeat every interval; its node gate accepts while the
    sandbox fits its disk, as direct_service does. A shared overlay models
    today's committed-route view, which C4.3 deletes with its serialization."""

    rng = random.Random(seed)
    clock = FakeClock()
    workers = []
    for index in range(scenario.nodes):
        loaded = index >= scenario.nodes - scenario.loaded_nodes
        worker = SimWorker(
            f"n{index:02d}", scenario.slots,
            base_cpu=rng.uniform(40.0, 60.0) if loaded else rng.uniform(5.0, 25.0),
        )
        worker.accepted = [
            SandboxIncarnation(f"old-{index}-{n}", 1, SPEC_HASH, f"old-{index}-{n}")
            for n in range(scenario.loaded_sandboxes if loaded else 0)
        ]
        workers.append(worker)
    by_job = {f"job-{worker.node}": worker for worker in workers}
    events = []
    for worker in workers:
        worker.publish(0.0)
        phase = rng.uniform(0.0, scenario.heartbeat_seconds)
        for beat in range(int(scenario.seconds / scenario.heartbeat_seconds) + 2):
            heapq.heappush(events, (phase + beat * scenario.heartbeat_seconds, 0, worker.node))
    for index in range(scenario.creates):
        heapq.heappush(events, (index * scenario.seconds / scenario.creates, 1, index))
    overlays = (
        [InflightOverlay(clock=clock)] * scenario.processes if shared_overlay
        else [InflightOverlay(clock=clock) for _ in range(scenario.processes)]
    )
    choosers = [
        policy(overlay, random.Random(seed * 1000 + index), scenario.target, scenario.processes)
        for index, overlay in enumerate(overlays)
    ]
    resident = {worker.node: 1.0 for worker in workers[: scenario.resident_nodes]}
    request = PlacementRequest(SHAPE, image="img", residency=resident)
    excess, unplaced = [], 0
    while events:
        clock.now, kind, payload = heapq.heappop(events)
        if kind == 0:
            next(w for w in workers if w.node == payload).publish(clock.now)
            continue
        process = rng.randrange(scenario.processes)
        chooser, overlay = choosers[process], overlays[process]
        view = FleetView(tuple(worker.published for worker in workers))
        item, excluded, placed = incarnation(payload), set(), False
        while not placed:
            ranked = chooser.choose(view, replace(request, excluded_job_ids=frozenset(excluded)))
            if not ranked:
                unplaced += 1
                break
            for node in ranked:
                overlay.reserve(node, item, request)
                worker = by_job[node.job_id]
                if len(worker.accepted) < worker.slots:
                    worker.accepted.append(item)
                    heapq.heappush(worker.finishes, clock.now + rng.uniform(*scenario.startup_seconds))
                    placed = True
                    break
                worker.rejected += 1
                overlay.release(node.job_id, item)
                excluded.add(node.job_id)
        # Herding: how far each worker's concurrent creates exceed the mean.
        creating = [worker.creating(clock.now) for worker in workers]
        average = sum(creating) / len(creating)
        excess.extend(value - average for value in creating)
    excess.sort()
    return {
        "overshoot": round(excess[-1], 1),
        "excess_p50": round(excess[len(excess) // 2], 1),
        "excess_p99": round(excess[int(len(excess) * 0.99)], 1),
        "rejections": sum(worker.rejected for worker in workers),
        "max_node_rejections": max(worker.rejected for worker in workers),
        "unplaced": unplaced,
        "counts": [len(worker.accepted) for worker in workers],
    }


SCENARIOS = (
    Scenario("burst"),
    Scenario("32-processes", processes=32),
    Scenario("scale-up", loaded_nodes=9, loaded_sandboxes=96),
    Scenario("resident-image", resident_nodes=2),
    Scenario("near-full", slots=64),
)
SEEDS = range(3)
POLICIES = (("lexicographic", LexicographicRanking), ("power-of-3", power_of_k))


def summarize(runs) -> dict:
    return {
        "overshoot": max(run["overshoot"] for run in runs),
        "excess_p99": max(run["excess_p99"] for run in runs),
        "rejections": sum(run["rejections"] for run in runs),
        "max_node_rejections": max(run["max_node_rejections"] for run in runs),
        "unplaced": sum(run["unplaced"] for run in runs),
    }


class HerdingSimulationTests(unittest.TestCase):
    """N processes with independent overlays place a burst on M workers."""

    @classmethod
    def setUpClass(cls):
        cls.results = {
            (scenario.name, name): [simulate(policy, scenario, seed) for seed in SEEDS]
            for scenario in SCENARIOS for name, policy in POLICIES
        }
        if os.environ.get("PLACEMENT_SIMULATION_REPORT"):
            reference = {
                (scenario.name, "shared-routes"): [
                    simulate(LexicographicRanking, scenario, seed, shared_overlay=True)
                    for seed in SEEDS
                ]
                for scenario in SCENARIOS
            }
            print(simulation_report(cls.results | reference))

    def compare(self, scenario):
        return (summarize(self.results[(scenario.name, "lexicographic")]),
                summarize(self.results[(scenario.name, "power-of-3")]))

    def test_power_of_k_herds_less_than_the_lexicographic_minimum(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario.name):
                old, new = self.compare(scenario)
                self.assertEqual(new["unplaced"], old["unplaced"])
                self.assertLess(2 * new["overshoot"], old["overshoot"])
                self.assertLess(2 * new["excess_p99"], old["excess_p99"])

    def test_power_of_k_overbooks_no_more_than_the_lexicographic_minimum(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario.name):
                old, new = self.compare(scenario)
                self.assertLessEqual(2 * new["rejections"], old["rejections"])
                self.assertLessEqual(new["max_node_rejections"], old["max_node_rejections"])
                if scenario.slots * scenario.nodes >= 2 * scenario.creates:
                    self.assertEqual(new["rejections"], 0)

    def test_simulation_is_deterministic(self):
        self.assertEqual(
            simulate(power_of_k, SCENARIOS[0], 0), simulate(power_of_k, SCENARIOS[0], 0),
        )


def simulation_report(results) -> str:
    lines = [
        f"{'scenario':<15} {'policy':<14} {'overshoot':>9} {'p99':>5} {'p50':>5} "
        f"{'rejects':>7} {'max/node':>8}  accepted per node (seed 0, sorted)"
    ]
    for (scenario, policy), runs in results.items():
        summary = summarize(runs)
        lines.append(
            f"{scenario:<15} {policy:<14} {summary['overshoot']:>9} "
            f"{summary['excess_p99']:>5} {median(run['excess_p50'] for run in runs):>5} "
            f"{summary['rejections']:>7} {summary['max_node_rejections']:>8}  "
            f"{sorted(runs[0]['counts'])}"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    unittest.main()
