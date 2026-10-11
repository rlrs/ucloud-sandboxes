"""The admission service suites over both backends (docs/admission-port-3c.md, 3c.3).

A suite class mixes in ``AdmissionBackendCase`` and passes each service it
builds through ``admitted``. Its subclass with ``noded = True`` runs every
test again with ``NodedAdmission`` against a real ``ucloud-noded
--admission-only`` (tests/noded_admission.py). The helpers read the ledger
the way both backends expose it (the transition and demand snapshots, the
registry's durable rows, each pool's slots), never the agent's private
ledger, so a test asserts the same thing in both modes.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
import secrets
import time

from tests import noded_admission
from ucloud_sandboxes.admission_backend import _cost, _owner
from ucloud_sandboxes.transition_admission import TransitionCost, TransitionKind


class AdmissionBackendCase:
    """Mixin for a ``unittest.TestCase`` suite of DirectSandboxService admission."""

    noded = False

    def admitted(self, service):
        """``service`` with this case's backend: its own ledger, or noded's.
        Services over one registry share one noded, as agent restarts do."""
        if self.noded:
            harness = getattr(self, "noded_harness", None)
            if harness is None or harness.registry_path != service.provisioner.registry.path:
                harness = None
            self.noded_harness = noded_admission.install(self, service, harness=harness)
        return service

    # -- the ledger, as both backends report it ---------------------------------

    @staticmethod
    def foreground_waiting(service) -> bool:
        """A create or restore waits for admission (``TransitionLedger.foreground_waiting``)."""
        transitions = service.transition_admission_snapshot()
        return bool(transitions["startup"]["waiting"] or transitions["restore"]["waiting"])

    def wait_for_foreground(self, service, timeout: float = 1.0) -> None:
        deadline = time.monotonic() + timeout
        while not self.foreground_waiting(service) and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertTrue(self.foreground_waiting(service))

    @staticmethod
    def known_memory_bytes(service) -> int:
        """Admitted memory: claims and forecasts (``known_memory_bytes``)."""
        return service.resident_demand_snapshot()["admitted_demand_bytes"]

    @staticmethod
    def intents(service) -> dict:
        """The durable growth rows by key (the agent's cache, or noded's, follows them)."""
        return {(item.sandbox_id, item.generation): item for item in service.provisioner.registry.growth_intents()}

    def observe_sample(self, service, key, sample) -> None:
        """What a sampler pass publishes: the sample, then refreshed forecasts.
        noded reads the sample as its next pass, before the next call."""
        service._resident_memory._samples[key] = sample
        if not self.noded:
            with service._capacity_guard:
                service._refresh_growth_forecasts_locked()

    # -- slots -------------------------------------------------------------------

    def queued(self, service, pool: str) -> int:
        """Waiters queued for a slot of ``pool`` (startup, restore, publication)."""
        if self.noded:
            return service.admission.slots()[pool]["waiting"]
        return len(getattr(service, f"_{pool}_slots")._waiters)

    def slots_available(self, service, pool: str) -> int:
        if self.noded:
            return service.admission.slots()[pool]["available"]
        return getattr(service, f"_{pool}_slots")._available

    def slots_capacity(self, service, pool: str) -> int:
        if self.noded:
            return service.admission.slots()[pool]["capacity"]
        return getattr(service, f"_{pool}_slots").capacity

    def wait_queued(self, service, pool: str, count: int) -> None:
        deadline = time.monotonic() + 3
        while self.queued(service, pool) != count:
            if time.monotonic() > deadline:
                raise AssertionError("admission did not queue")
            time.sleep(0.005)

    @contextmanager
    def held_slots(self, service, pool: str, count: int | None = None):
        """Hold ``count`` slots of ``pool`` (all of them by default), as busy
        peers do. noded's are held as reasserted claims of no cost."""
        count = self.slots_capacity(service, pool) if count is None else count
        if not self.noded:
            limiter = getattr(service, f"_{pool}_slots")
            for index in range(count):
                assert limiter.acquire(timeout=1, owner=(f"held-{pool}-{index}", 1))
            try:
                yield
            finally:
                for _ in range(count):
                    limiter.release()
            return
        backend = service.admission
        kind = TransitionKind(pool)
        cost = TransitionCost(kind, 0, provenance="held-by-test")
        claims = {}
        for index in range(count):
            claims[secrets.token_hex(16)] = {"owner": _owner((f"held-{pool}-{index}", 1)), "kind": pool,
                                             "cost": _cost(cost), "requested": None}
        with backend._guard:
            backend._claims.update(claims)
        backend.link.hello_now()
        with ExitStack() as release:
            for token in claims:
                release.callback(backend._release, token)
            yield
