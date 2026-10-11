"""Property traces for node admission's FIFO slots and transition ledger.

docs/admission-port-3c.md step 3c.1: C5.3 carries FairCapacity and
TransitionLedger over to ucloud-noded unchanged, so their invariants are
stated here as properties first. The harnesses below also record the Python
goldens the Rust port replays (runtime/noded/src/admission/testdata/goldens.py).

FairHarness drives the real FairCapacity with one thread per blocking
acquire. Each waiter's ``ready`` event is a gate: a grant sets it as usual,
but the waiter returns from ``wait`` only when the harness says so (observe:
a wakeup or a timeout, which run the same code; interrupt: an exception).
So a grant and its waiter's return are separate steps, and the grant/timeout
race is an ordinary trace.
"""

from contextlib import contextmanager
from dataclasses import replace
import threading
import time
import unittest
from unittest.mock import patch

from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from ucloud_sandboxes import admission
from ucloud_sandboxes.admission import FairCapacity
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.transition_admission import TransitionCost, TransitionKind, TransitionLedger


# FairCapacity.acquire looks _Waiter up at call time; the harness replaces it.
_WAITER = admission._Waiter


class _Gate(threading.Event):
    """A waiter's ``ready``: set by grants, released only by the harness."""

    def __init__(self):
        super().__init__()
        self._control = threading.Condition()
        self.entered = False
        self.action = None

    def wait(self, timeout=None):
        with self._control:
            self.entered = True
            self._control.notify_all()
            while self.action is None:
                self._control.wait()
            action = self.action
        if action == "interrupt":
            raise KeyboardInterrupt
        return self.is_set()

    def act(self, action):
        with self._control:
            self.action = action
            self._control.notify_all()


class FairHarness:
    """One FairCapacity; blocking acquires queue on their own threads."""

    def __init__(self, capacity):
        self.capacity = FairCapacity(capacity)
        self.pending = {}  # id -> {"thread", "ticket", "done", "box", "weight", "owner", "seq"}
        self._tickets = {}
        self._next = 0
        self._patch = patch.object(admission, "_Waiter", self._waiter)

    def _waiter(self, weight, owner=None):
        ticket = _WAITER(weight, owner=owner, ready=_Gate())
        self._tickets[threading.get_ident()] = ticket
        return ticket

    def __enter__(self):
        self._patch.start()
        return self

    def __exit__(self, *_exc):
        for ident in list(self.pending):
            self.interrupt(ident)
        self._patch.stop()

    def acquire(self, weight, owner, blocking=True):
        """("granted" | "refused" | "queued", id or None)."""
        if not blocking:
            return ("granted" if self.capacity.acquire(blocking=False, weight=weight, owner=owner)
                    else "refused"), None
        self._next += 1
        ident, box, done = self._next, {}, threading.Event()

        def run():
            # Thread identifiers are reused: drop a finished thread's ticket.
            self._tickets.pop(threading.get_ident(), None)
            box["ident"] = threading.get_ident()
            try:
                box["result"] = self.capacity.acquire(timeout=60, weight=weight, owner=owner)
            except KeyboardInterrupt:
                box["result"] = "interrupted"
            finally:
                done.set()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 5
        while True:
            if done.wait(0.0005):
                thread.join()
                assert box["result"] is True, box
                return "granted", None
            ticket = self._tickets.get(box.get("ident"))
            if ticket is not None and ticket.ready.entered:
                break
            if time.monotonic() > deadline:
                raise AssertionError("acquire neither returned nor queued")
        self.pending[ident] = {"thread": thread, "ticket": ticket, "done": done, "box": box,
                               "weight": weight, "owner": owner, "seq": ident}
        return "queued", ident

    def _finish(self, ident, action):
        item = self.pending.pop(ident)
        item["ticket"].ready.act(action)
        item["thread"].join(5)
        assert not item["thread"].is_alive(), "waiter did not return"
        return item["box"]["result"]

    def observe(self, ident):
        """The waiter wakes or times out (the same code path): its return."""
        return self._finish(ident, "observe")

    def interrupt(self, ident):
        return self._finish(ident, "interrupt")

    def ids(self, tickets):
        by_ticket = {id(item["ticket"]): ident for ident, item in self.pending.items()}
        return [by_ticket[id(ticket)] for ticket in tickets]

    def state(self):
        with self.capacity._condition:
            return {
                "available": self.capacity._available,
                "queue": self.ids(self.capacity._waiters),
                "tickets": {str(ident): {"granted": item["ticket"].granted,
                                         "cancelled": item["ticket"].cancelled,
                                         "ready": item["ticket"].ready.is_set()}
                            for ident, item in sorted(self.pending.items())},
            }


@contextmanager
def fair_harness(capacity):
    with FairHarness(capacity) as harness:
        yield harness


class FairCapacityMachine(RuleBasedStateMachine):
    """Random acquire, release, cancel, timeout and interrupt traces."""

    CAPACITY = 6
    OWNERS = ("a", "b", "c")

    def __init__(self):
        super().__init__()
        self.harness = FairHarness(self.CAPACITY).__enter__()
        self.held = []  # weights the callers own: immediate grants and observed True
        self.granted_seen = set()

    def teardown(self):
        self.harness.__exit__(None, None, None)
        assert self.harness.capacity._available == self.CAPACITY - sum(self.held)

    def granted_pending(self):
        return sum(item["weight"] for item in self.harness.pending.values() if item["ticket"].granted)

    @rule(weight=st.integers(1, CAPACITY), owner=st.sampled_from(OWNERS), blocking=st.booleans())
    def acquire(self, weight, owner, blocking):
        capacity = self.harness.capacity
        queued_before = bool(capacity._waiters)
        available_before = capacity._available
        outcome, ident = self.harness.acquire(weight, owner, blocking)
        if outcome == "granted":
            # No barging: a caller passes the queue only when it is empty.
            assert not queued_before and available_before >= weight
            self.held.append(weight)
        elif outcome == "refused":
            assert queued_before or available_before < weight
        else:
            assert queued_before or available_before < weight
            assert capacity._waiters[-1] is self.harness.pending[ident]["ticket"]

    @rule(data=st.data())
    @precondition(lambda self: self.held)
    def release(self, data):
        weight = self.held.pop(data.draw(st.integers(0, len(self.held) - 1)))
        self.harness.capacity.release(weight=weight)

    @rule(weight=st.integers(1, CAPACITY))
    def release_without_reservation(self, weight):
        capacity = self.harness.capacity
        if capacity._available + weight <= self.CAPACITY:
            return
        before = self.harness.state()
        with self.assertRaises(ValueError):
            capacity.release(weight=weight)
        assert self.harness.state() == before

    @rule(owner=st.sampled_from((None, *OWNERS)))
    def cancel(self, owner):
        granted = {ident for ident, item in self.harness.pending.items() if item["ticket"].granted}
        expected = sum(owner is None or ticket.owner == owner for ticket in self.harness.capacity._waiters)
        assert self.harness.capacity.cancel_waiters(owner) == expected
        # Cancel never revokes a grant.
        assert granted <= {ident for ident, item in self.harness.pending.items() if item["ticket"].granted}

    @rule(data=st.data())
    @precondition(lambda self: self.harness.pending)
    def observe(self, data):
        ident = data.draw(st.sampled_from(sorted(self.harness.pending)))
        item = self.harness.pending[ident]
        granted = item["ticket"].granted
        result = self.harness.observe(ident)
        # A grant racing the timeout is never lost; a non-grant never leaks.
        assert result is granted
        if result:
            self.held.append(item["weight"])
        assert item["ticket"] not in self.harness.capacity._waiters

    @rule(data=st.data())
    @precondition(lambda self: self.harness.pending)
    def interrupt(self, data):
        ident = data.draw(st.sampled_from(sorted(self.harness.pending)))
        ticket = self.harness.pending[ident]["ticket"]
        assert self.harness.interrupt(ident) == "interrupted"
        # The interrupted waiter returns a granted slot (conservation below).
        assert ticket not in self.harness.capacity._waiters

    @invariant()
    def weight_is_conserved(self):
        capacity = self.harness.capacity
        assert capacity._available + sum(self.held) + self.granted_pending() == self.CAPACITY

    @invariant()
    def head_is_never_stranded(self):
        capacity = self.harness.capacity
        assert not capacity._waiters or capacity._waiters[0].weight > capacity._available

    @invariant()
    def grants_follow_fifo(self):
        queued = [item["seq"] for item in self.harness.pending.values()
                  if item["ticket"] in self.harness.capacity._waiters]
        assert queued == sorted(queued)
        for ident, item in self.harness.pending.items():
            ticket = item["ticket"]
            assert not (ticket.granted and ticket.cancelled)
            assert ticket.ready.is_set() == (ticket.granted or ticket.cancelled)
            assert (ticket in self.harness.capacity._waiters) == (not ticket.granted and not ticket.cancelled)
            if ticket.granted and ident not in self.granted_seen:
                # Nothing still queued was enqueued before a new grant.
                assert all(seq > item["seq"] for seq in queued)
                self.granted_seen.add(ident)

    def assertRaises(self, error):
        return unittest.TestCase().assertRaises(error)


GIB = 1024**3
OWNERS = tuple(("s", index) for index in range(5))
VALUES = (None, 0, 512, 1024, 4096, 2 * GIB)
KINDS = tuple(TransitionKind)
LIMITS = {TransitionKind.STARTUP: 2, TransitionKind.RESTORE: 1}

costs = st.builds(
    TransitionCost, st.sampled_from(KINDS), st.sampled_from(VALUES),
    read_bytes=st.sampled_from(VALUES), write_bytes=st.sampled_from(VALUES),
    ram_backing_bytes=st.sampled_from(VALUES))
requested = st.one_of(st.none(), st.builds(ResourceQuantity, vcpu=st.sampled_from((0.0, 0.5, 2.0)),
                                           memory_mb=st.sampled_from((0, 512, 4096)), disk_mb=st.just(0)))


def recount(claims, forecasts, resource="memory_bytes", extra=()):
    """Per owner, the max of its forecast, its claims and ``extra``; summed."""
    peaks = {}
    pairs = [*((owner, cost) for owner, cost in forecasts.items() if cost is not None),
             *((claim.owner, claim.cost) for claim in claims.values())]
    for owner, value in [*((owner, getattr(cost, resource)) for owner, cost in pairs), *extra]:
        if value is not None:
            peaks[owner] = max(peaks.get(owner, 0), value)
    return sum(peaks.values())


def pending(claims, waits, kind):
    """First wait per owner of this kind, FIFO, skipping owners already active in it."""
    active = {claim.owner for claim in claims.values() if claim.cost.kind == kind}
    owners = {}
    for token in sorted(waits):
        item = waits[token]
        if item.cost.kind == kind and item.owner not in active:
            owners.setdefault(item.owner, item)
    return list(owners.values())


class TransitionLedgerMachine(RuleBasedStateMachine):
    """Random claim, wait, requote and forecast traces against a recount."""

    def __init__(self):
        super().__init__()
        self.ledger = TransitionLedger()
        self.claims = {}
        self.waits = {}
        self.forecasts = {}

    @rule(owner=st.sampled_from(OWNERS), cost=costs)
    def wait(self, owner, cost):
        token = self.ledger.wait(owner, cost)
        assert token not in self.waits and token not in self.claims
        self.waits[token] = self.ledger._waiting[token]

    @rule(data=st.data())
    @precondition(lambda self: self.waits)
    def unwait(self, data):
        token = data.draw(st.sampled_from(sorted(self.waits)))
        self.ledger.unwait(token)
        self.ledger.unwait(token)  # idempotent
        del self.waits[token]

    @rule(owner=st.sampled_from(OWNERS), cost=costs)
    def refresh_wait_cost(self, owner, cost):
        self.ledger.refresh_wait_cost(owner, cost)
        for token, item in self.waits.items():
            if item.owner == owner and item.cost.kind == cost.kind:
                self.waits[token] = replace(item, cost=cost)

    @rule(owner=st.sampled_from(OWNERS), cost=costs, shape=requested)
    def claim(self, owner, cost, shape):
        claim = self.ledger.claim(owner, cost, requested=shape)
        assert claim.token not in self.claims and claim.token not in self.waits
        self.claims[claim.token] = claim

    @rule(data=st.data())
    @precondition(lambda self: self.claims)
    def release(self, data):
        token = data.draw(st.sampled_from(sorted(self.claims)))
        claim = self.claims.pop(token)
        # A peer's token, or a forged copy, never releases this claim.
        for forged in (replace(claim, owner=("forged", 0)), replace(claim, token=claim.token + 10**6)):
            with unittest.TestCase().assertRaises(ValueError):
                self.ledger.release(forged)
        self.ledger.release(claim)
        with unittest.TestCase().assertRaises(ValueError):
            self.ledger.release(claim)

    @rule(owner=st.sampled_from(OWNERS), cost=st.one_of(st.none(), costs))
    def set_forecast(self, owner, cost):
        self.ledger.set_growth_forecast(owner, cost)
        self.forecasts[owner] = cost

    @rule(forecasts=st.dictionaries(st.sampled_from(OWNERS), costs, max_size=3))
    def set_forecasts(self, forecasts):
        self.ledger.set_growth_forecasts(forecasts)
        self.forecasts = dict(forecasts)

    @invariant()
    def running_totals_equal_a_recount(self):
        live = {owner: cost for owner, cost in self.forecasts.items() if cost is not None}
        for resource in ("memory_bytes", "ram_backing_bytes"):
            assert self.ledger._growth_totals[resource] == sum(getattr(c, resource) or 0 for c in live.values())
            assert self.ledger._total(resource) == recount(self.claims, self.forecasts, resource)
        assert self.ledger._growth_unknown == sum(c.memory_bytes is None for c in live.values())
        assert self.ledger.known_memory_bytes == recount(self.claims, self.forecasts)

    @invariant()
    def per_owner_charge_is_a_max_not_a_sum(self):
        for owner in OWNERS:
            for kind in (TransitionKind.RESTORE, TransitionKind.CAPTURE):
                for value in (0, 1024, 2 * GIB):
                    cost = TransitionCost(kind, value, ram_backing_bytes=value)
                    for resource in ("memory_bytes", "ram_backing_bytes"):
                        projected = self.ledger.projected_memory_bytes(owner, cost, resource=resource)
                        assert projected == recount(self.claims, self.forecasts, resource, [(owner, value)])
                        # An owner already charged at least this much pays nothing more.
                        assert projected >= self.ledger._total(resource)

    @invariant()
    def startup_spends_only_what_queued_restores_leave(self):
        restores = pending(self.claims, self.waits, TransitionKind.RESTORE)
        for owner in (OWNERS[0], ("new", 1)):
            cost = TransitionCost(TransitionKind.STARTUP, 1024, ram_backing_bytes=512)
            for resource in ("memory_bytes", "ram_backing_bytes"):
                extra = [(owner, getattr(cost, resource)),
                         *((item.owner, getattr(item.cost, resource)) for item in restores)]
                assert self.ledger.projected_memory_bytes(owner, cost, resource=resource) == recount(
                    self.claims, self.forecasts, resource, extra)

    @invariant()
    def reclaim_bills_admitted_work_and_the_next_eligible_owner(self):
        for limits in (LIMITS, {TransitionKind.STARTUP: 0, TransitionKind.RESTORE: 0}):
            for resource in ("memory_bytes", "ram_backing_bytes"):
                expected = recount(self.claims, self.forecasts, resource)
                for kind in (TransitionKind.RESTORE, TransitionKind.STARTUP):
                    active = sum(claim.cost.kind == kind for claim in self.claims.values())
                    queue = pending(self.claims, self.waits, kind)
                    if active < limits.get(kind, 0) and queue:
                        expected = recount(self.claims, self.forecasts, resource,
                                           [(queue[0].owner, getattr(queue[0].cost, resource))])
                        break
                assert self.ledger.next_memory_demands(limits, resource=resource) == expected
            snapshot = self.ledger.demand_snapshot(limits)
            assert snapshot["admitted_demand_bytes"] == recount(self.claims, self.forecasts)
            assert snapshot["pending_demand_bytes"] >= 0
        assert self.ledger.foreground_waiting == any(
            pending(self.claims, self.waits, kind) for kind in (TransitionKind.STARTUP, TransitionKind.RESTORE))

    @invariant()
    def snapshot_counts_claims_and_queued_owners(self):
        snapshot = self.ledger.snapshot()
        for kind in KINDS:
            costs_ = [claim.cost for claim in self.claims.values() if claim.cost.kind == kind]
            assert snapshot[kind.value]["active"] == len(costs_) == self.ledger.active_count(kind)
            assert snapshot[kind.value]["waiting"] == len(pending(self.claims, self.waits, kind))
            memory = [cost.memory_bytes for cost in costs_]
            assert snapshot[kind.value]["memory_bytes"] == (None if None in memory else sum(memory))
        reservations = {}
        for claim in sorted(self.claims.values(), key=lambda c: c.token):
            if claim.requested is not None:
                reservations[claim.owner] = claim.requested
        assert self.ledger.resource_reservations() == reservations


TestFairCapacityProperties = FairCapacityMachine.TestCase
TestFairCapacityProperties.settings = settings(max_examples=60, stateful_step_count=30, deadline=None,
                                               derandomize=True)
TestTransitionLedgerProperties = TransitionLedgerMachine.TestCase
TestTransitionLedgerProperties.settings = settings(max_examples=150, stateful_step_count=40, deadline=None,
                                                   derandomize=True)


class FairHarnessTests(unittest.TestCase):
    def test_grant_racing_timeout_is_an_ordinary_trace(self):
        with fair_harness(1) as harness:
            self.assertEqual(harness.acquire(1, "a"), ("granted", None))
            outcome, waiter = harness.acquire(1, "b")
            self.assertEqual(outcome, "queued")
            harness.capacity.release()
            # Granted before the waiter's timeout fires: the timeout keeps it.
            self.assertTrue(harness.state()["tickets"][str(waiter)]["granted"])
            self.assertIs(harness.observe(waiter), True)
            self.assertEqual(harness.capacity._available, 0)
            harness.capacity.release()

    def test_interrupted_grant_returns_its_weight(self):
        with fair_harness(4) as harness:
            harness.acquire(4, "a")
            _, head = harness.acquire(3, "b")
            _, tail = harness.acquire(2, "c")
            harness.capacity.release(weight=4)
            self.assertEqual(harness.state()["queue"], [tail])
            self.assertEqual(harness.interrupt(head), "interrupted")
            self.assertEqual(harness.state()["queue"], [])
            self.assertIs(harness.observe(tail), True)
            self.assertEqual(harness.capacity._available, 2)
