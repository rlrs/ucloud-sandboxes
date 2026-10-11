"""The pure node admission decision: fences, live memory, projections, deadlines."""
from dataclasses import replace
import unittest

from ucloud_sandboxes.admission_decision import (
    CLOSED,
    PROJECTED,
    PROJECTED_RAM,
    RETRY_SECONDS,
    SHAPE,
    AdmissionNode,
    AdmissionRequest,
    Grant,
    Refuse,
    Wait,
    decide,
    gate,
)
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.resource_evidence import MemoryBackingCapacity
from ucloud_sandboxes.sandbox import SandboxAdmissionClosedError, SandboxCapacityUnavailableError
from ucloud_sandboxes.transition_admission import TransitionCost, TransitionKind, TransitionLedger

MIB, GIB = 1024**2, 1024**3
SHAPE_64G = ResourceQuantity(vcpu=8.0, memory_mb=65536)
OWNER, OTHER = ("a", 1), ("b", 1)
NO_METRICS = "direct node has no fresh runtime metrics for dynamic admission"
LIVE = "direct node has insufficient live memory headroom"
PHYSICAL = "direct node has insufficient physical live memory headroom"
PSI = "direct node memory pressure blocks active admission"


def metrics(available_mb=60000, *, total_mb=65536, swap_total_mb=0, swap_free_mb=0, psi=0.0, backing=None,
            cpu_percent=None):
    return NodeRuntimeMetrics(
        collected_at=utc_now(), memory_total_mb=total_mb, memory_available_mb=available_mb,
        swap_total_mb=swap_total_mb, swap_free_mb=swap_free_mb, memory_psi_full_avg10=psi,
        memory_backing=backing, cpu_percent=cpu_percent)


def cost(kind=TransitionKind.STARTUP, memory=GIB, ram=None):
    return TransitionCost(kind, memory, ram_backing_bytes=ram, provenance="test")


def request(*, cost=None, capacity=SHAPE_64G, open=True, ram=False, requested=None, check_shape=True,
            check_memory_pressure=True, now=100.0, deadline=130.0, previous_error=None):
    return AdmissionRequest(
        node=AdmissionNode(open, capacity, ram_backing_configured=ram), owner=OWNER,
        requested=requested or ResourceQuantity(vcpu=1.0, memory_mb=1024), cost=cost, check_shape=check_shape,
        check_memory_pressure=check_memory_pressure, deadline=deadline, now=now, previous_error=previous_error)


class FenceTests(unittest.TestCase):
    def test_closed_admission_refuses_with_the_closed_error(self):
        for decision in (gate(request(open=False)), decide(metrics(), TransitionLedger(), request(open=False))):
            self.assertEqual(decision, Refuse(CLOSED, closed=True))
            self.assertIsInstance(decision.error(), SandboxAdmissionClosedError)
            self.assertEqual(str(decision.error()), CLOSED)

    def test_a_request_beyond_the_node_shape_refuses_only_when_shape_is_checked(self):
        large = ResourceQuantity(vcpu=16.0, memory_mb=512)
        refused = decide(metrics(), TransitionLedger(), request(requested=large))
        self.assertEqual(refused, Refuse(SHAPE))
        self.assertIsInstance(refused.error(), SandboxCapacityUnavailableError)
        self.assertEqual(gate(request(requested=large)), Refuse(SHAPE))
        self.assertEqual(decide(metrics(), TransitionLedger(), request(requested=large, check_shape=False)), Grant())
        # An empty request does not fit a shape either (dynamic_request_fits).
        self.assertEqual(gate(request(requested=ResourceQuantity())), Refuse(SHAPE))

    def test_gate_refuses_a_retry_whose_deadline_passed(self):
        self.assertIsNone(gate(request(previous_error=LIVE, now=129.9)))
        self.assertEqual(gate(request(previous_error=LIVE, now=130.0)), Refuse(LIVE))
        self.assertIsNone(gate(request(now=500.0)))


class LiveMemoryTests(unittest.TestCase):
    def test_without_a_node_shape_admission_is_slot_only(self):
        self.assertEqual(decide(None, TransitionLedger(), request(capacity=None, cost=cost())), Grant())
        late = request(capacity=None, previous_error=PROJECTED, now=131.0)
        self.assertEqual(decide(None, TransitionLedger(), late), Refuse(PROJECTED))

    def test_missing_metrics_refuse_at_once_even_for_a_restore(self):
        for item in (None, cost(TransitionKind.RESTORE)):
            self.assertEqual(decide(None, TransitionLedger(), request(cost=item)), Refuse(NO_METRICS))

    def test_cpu_pressure_never_refuses_node_admission(self):
        busy = replace(metrics(), cpu_percent=99.0, load_average_1m=1000.0, cpu_count=8)
        self.assertEqual(decide(busy, TransitionLedger(), request()), Grant())
        self.assertEqual(decide(busy, TransitionLedger(), request(cost=cost())), Grant())

    def test_the_physical_floor_refuses_live_checks_and_queues_startups_and_restores(self):
        low = metrics(2047, swap_total_mb=8192, swap_free_mb=8192)
        self.assertEqual(decide(low, TransitionLedger(), request()), Refuse(PHYSICAL))
        for kind in (TransitionKind.CAPTURE, TransitionKind.PUBLICATION):
            # Free swap never satisfies the floor; only startups and restores
            # wait for memory to return.
            self.assertEqual(decide(low, TransitionLedger(), request(cost=cost(kind, None))), Refuse(PHYSICAL))
        for kind in (TransitionKind.STARTUP, TransitionKind.RESTORE):
            self.assertEqual(decide(low, TransitionLedger(), request(cost=cost(kind, 0))),
                             Wait(PHYSICAL, RETRY_SECONDS))

    def test_memory_psi_applies_only_when_checked(self):
        stalled = metrics(psi=10.0)
        self.assertEqual(decide(stalled, TransitionLedger(), request()), Refuse(PSI))
        self.assertEqual(decide(stalled, TransitionLedger(), request(check_memory_pressure=False)), Grant())
        self.assertEqual(decide(metrics(psi=9.99), TransitionLedger(), request()), Grant())
        self.assertEqual(decide(stalled, TransitionLedger(), request(cost=cost(TransitionKind.RESTORE))),
                         Wait(PSI, RETRY_SECONDS))

    def test_live_pressure_is_judged_by_the_transition_footprint_when_known(self):
        node = metrics(3000)
        large = ResourceQuantity(vcpu=1.0, memory_mb=4096)
        self.assertEqual(decide(node, TransitionLedger(), request(requested=large)), Refuse(LIVE))
        tiny = cost(TransitionKind.RESTORE, 1)
        self.assertEqual(decide(node, TransitionLedger(), request(requested=large, cost=tiny)), Grant())
        # Swap counts toward the live headroom, never toward the floor.
        swapped = metrics(3000, swap_total_mb=8192, swap_free_mb=2000)
        self.assertEqual(decide(swapped, TransitionLedger(), request(requested=large)), Grant())


class ProjectionTests(unittest.TestCase):
    def test_in_flight_claims_spend_the_headroom_above_the_floor_once(self):
        ledger = TransitionLedger()
        node = metrics(2048 + 4096)
        self.assertEqual(decide(node, ledger, request(cost=cost(memory=4 * GIB))), Grant())
        ledger.claim(OTHER, cost(memory=3 * GIB))
        self.assertEqual(decide(node, ledger, request(cost=cost(memory=GIB))), Grant())
        self.assertEqual(decide(node, ledger, request(cost=cost(memory=GIB + 1))), Wait(PROJECTED, RETRY_SECONDS))

    def test_an_owner_is_charged_the_max_of_its_demand_never_the_sum(self):
        ledger = TransitionLedger()
        ledger.wait(OWNER, cost(memory=4 * GIB))
        self.assertEqual(decide(metrics(2048 + 4096), ledger, request(cost=cost(memory=4 * GIB))), Grant())

    def test_a_startup_leaves_every_queued_restore_its_headroom(self):
        ledger = TransitionLedger()
        ledger.wait(OTHER, cost(TransitionKind.RESTORE, 3 * GIB))
        node = metrics(2048 + 4096)
        self.assertEqual(decide(node, ledger, request(cost=cost(memory=2 * GIB))), Wait(PROJECTED, RETRY_SECONDS))
        self.assertEqual(decide(node, ledger, request(cost=cost(TransitionKind.RESTORE, 2 * GIB))), Grant())

    def test_unknown_memory_total_is_a_projected_wait(self):
        unknown = metrics(60000, total_mb=0)
        self.assertEqual(decide(unknown, TransitionLedger(), request(cost=cost(TransitionKind.CAPTURE, None))),
                         Wait(PROJECTED, RETRY_SECONDS))

    def test_configured_ram_backing_without_evidence_counts_as_none_available(self):
        ram_cost = cost(memory=GIB, ram=GIB)
        self.assertEqual(decide(metrics(), TransitionLedger(), request(cost=ram_cost)), Grant())
        self.assertEqual(decide(metrics(), TransitionLedger(), request(cost=ram_cost, ram=True)),
                         Wait(PROJECTED_RAM, RETRY_SECONDS))
        unknown = metrics(backing=MemoryBackingCapacity(32 * GIB, None, "1:2:3"))
        self.assertEqual(decide(unknown, TransitionLedger(), request(cost=ram_cost)),
                         Wait(PROJECTED_RAM, RETRY_SECONDS))
        roomy = metrics(backing=MemoryBackingCapacity(32 * GIB, 2 * GIB, "1:2:3"))
        self.assertEqual(decide(roomy, TransitionLedger(), request(cost=ram_cost)), Grant())

    def test_a_file_backed_restore_spends_physical_bytes_but_no_ram_backing(self):
        full = metrics(backing=MemoryBackingCapacity(32 * GIB, 0, "1:2:3"))
        file_backed = cost(TransitionKind.RESTORE, GIB, ram=0)
        self.assertEqual(decide(full, TransitionLedger(), request(cost=file_backed, ram=True)), Grant())
        ledger = TransitionLedger()
        ledger.claim(OTHER, cost(memory=4 * GIB, ram=0))
        self.assertEqual(decide(metrics(2048 + 4096, backing=full.memory_backing), ledger,
                                request(cost=file_backed, ram=True)), Wait(PROJECTED, RETRY_SECONDS))


class DeadlineTests(unittest.TestCase):
    def test_a_wait_never_reaches_past_the_deadline(self):
        low = metrics(2047)
        startup = cost(memory=0)
        self.assertEqual(decide(low, TransitionLedger(), request(cost=startup, now=129.7)), Wait(PHYSICAL, 0.21))
        self.assertEqual(decide(low, TransitionLedger(), request(cost=startup, now=129.8)), Refuse(PHYSICAL))
        self.assertEqual(decide(low, TransitionLedger(), request(cost=startup, now=130.0, deadline=130.0)),
                         Refuse(PHYSICAL))

    def test_a_late_sample_after_a_wait_refuses_with_the_earlier_reason(self):
        late = request(cost=cost(), previous_error=PROJECTED, now=130.0)
        self.assertEqual(decide(metrics(), TransitionLedger(), late), Refuse(PROJECTED))
        on_time = replace(late, now=129.0)
        self.assertEqual(decide(metrics(), TransitionLedger(), on_time), Grant())

    def test_the_decision_is_pure(self):
        ledger = TransitionLedger()
        ledger.wait(OWNER, cost(TransitionKind.RESTORE, 8 * GIB))
        before = ledger.snapshot()
        item = request(cost=cost(TransitionKind.RESTORE, 8 * GIB))
        first = decide(metrics(4096), ledger, item)
        self.assertEqual(first, Wait(PROJECTED, RETRY_SECONDS))
        self.assertEqual(decide(metrics(4096), ledger, item), first)
        self.assertEqual(ledger.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
