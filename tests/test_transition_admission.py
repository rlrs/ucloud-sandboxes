"""Admission consumes each memory forecast once and never fences out cancellation."""
from ucloud_sandboxes.transition_admission import MemoryDemand

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_direct_provisioner as fixtures
from tests.test_startup_admission import wait_queued
from ucloud_sandboxes.admission import FairCapacity
from ucloud_sandboxes.background_io import Pressure
from ucloud_sandboxes.checkpoint_components import MemoryBackingRef, WorkspaceCaptureRef
from ucloud_sandboxes.direct_service import (
    DirectSandboxService,
    SandboxRestoreBusyError,
    SandboxCapacityUnavailableError,
)
from ucloud_sandboxes.direct_warden import DirectWardenError
from ucloud_sandboxes.direct_registry import DirectRegistryCapacityUnavailable
from ucloud_sandboxes.hibernation import (
    HibernationFileRole, HibernationManifest, HibernationState, LocalHibernationArtifactFile,
)
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.resource_evidence import MemoryBackingCapacity
from ucloud_sandboxes.warm_park import WarmParkPolicy, decide_resident_wait
from ucloud_sandboxes.transition_admission import (
    TransitionCost,
    TransitionKind,
    TransitionLedger,
)


class TransitionLedgerTests(unittest.TestCase):
    def test_reclaim_prices_next_owner_not_the_configured_slot_wave(self):
        ledger = TransitionLedger()
        gib = 1024**3
        limits = {TransitionKind.STARTUP: 8, TransitionKind.RESTORE: 8}
        for kind in limits:
            for index in range(8):
                ledger.wait((kind.value, index), TransitionCost(kind, 2 * gib))
        # Sixteen pending 2 GiB guarantees do not justify 32 GiB of evictions.
        self.assertEqual(ledger.next_memory_demands(limits), 2 * gib)
        pressure = Pressure(16 / 90, 0, 30, 16 * gib)
        self.assertFalse(decide_resident_wait(
            pressure, MemoryDemand(ledger.next_memory_demands(limits), ledger.next_memory_demands(limits))
        ).reclaim)
        active = ledger.claim(("restore", 0), TransitionCost(TransitionKind.RESTORE, 2 * gib))
        self.assertEqual(ledger.next_memory_demands(limits), 4 * gib)
        snapshot = ledger.demand_snapshot(limits)
        self.assertEqual(snapshot["admitted_demand_bytes"], 2 * gib)
        self.assertEqual(snapshot["pending_demand_bytes"], 2 * gib)
        self.assertEqual(snapshot["unknown_transition_memory_costs"], 0)
        ledger.release(active)

    def test_restore_head_protection_shares_owner_and_releases_on_cancel(self):
        ledger = TransitionLedger()
        restore = TransitionCost(TransitionKind.RESTORE, 4096)
        start = TransitionCost(TransitionKind.STARTUP, 2048)
        head = ledger.wait(("wake", 1), restore)
        ledger.wait(("wake", 2), restore)
        ledger.set_growth_forecasts({("wake", 1): TransitionCost(TransitionKind.RESTORE, 1024)})
        self.assertEqual(ledger.projected_memory_bytes(
            ("new", 1), start, restore_capacity=8,
        ), 6144)
        # The actual wake spends the same owner reservation, not both stages.
        self.assertEqual(ledger.projected_memory_bytes(("wake", 1), restore), 4096)
        ledger.unwait(head)
        self.assertEqual(ledger.projected_memory_bytes(
            ("new", 1), start, restore_capacity=8,
        ), 7168)
        # All active restore slots are already charged; a later queued owner
        # cannot cause speculative reclaim before a slot is available.
        claim = ledger.claim(("wake", 3), restore)
        self.assertEqual(ledger.projected_memory_bytes(
            ("new", 1), start, restore_capacity=1,
        ), 7168)
        ledger.release(claim)

    def test_unknown_cost_and_exact_token_lifetime(self):
        ledger = TransitionLedger()
        cost = TransitionCost(TransitionKind.PUBLICATION, None)
        claim = ledger.claim(("same", 1), cost)
        peer = ledger.claim(("same", 1), cost)
        self.assertIsNone(ledger.snapshot()["publication"]["memory_bytes"])
        with self.assertRaises(ValueError):
            ledger.release(replace(claim, owner=("wrong", 1)))
        self.assertEqual(ledger.active_count(TransitionKind.PUBLICATION), 2)
        ledger.release(claim)
        with self.assertRaises(ValueError):
            ledger.release(claim)
        self.assertEqual(ledger.active_count(TransitionKind.PUBLICATION), 1)
        ledger.release(peer)
        self.assertEqual(ledger.snapshot()["publication"]["memory_bytes"], 0)

    def test_duplicate_owner_forecast_is_shared_but_tokens_are_independent(self):
        ledger = TransitionLedger()
        cost = TransitionCost(TransitionKind.RESTORE, 1024)
        first = ledger.claim(("same", 1), cost)
        self.assertEqual(ledger.projected_memory_bytes(("same", 1), cost), 1024)
        second = ledger.claim(("same", 1), cost)
        self.assertEqual(ledger.known_memory_bytes, 1024)
        ledger.release(first)
        self.assertEqual(ledger.known_memory_bytes, 1024)
        ledger.release(second)
        self.assertEqual(ledger.known_memory_bytes, 0)

    def test_cancel_fifo_head_grants_next_without_revoking_active_owner(self):
        capacity = FairCapacity(4)
        capacity.acquire(weight=2, owner="active")
        with ThreadPoolExecutor(max_workers=2) as pool:
            head = pool.submit(capacity.acquire, timeout=3, weight=4, owner="deleted")
            wait_queued(capacity, 1)
            tail = pool.submit(capacity.acquire, timeout=3, weight=2, owner="tail")
            wait_queued(capacity, 2)
            self.assertEqual(capacity.cancel_waiters("active"), 0)
            self.assertEqual(capacity.cancel_waiters("deleted"), 1)
            self.assertFalse(head.result(1))
            self.assertTrue(tail.result(1))
        capacity.release(weight=2)
        capacity.release(weight=2)
        self.assertTrue(capacity.acquire(weight=4, blocking=False))
        capacity.release(weight=4)


class TransitionServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.fixture = fixtures.DirectProvisionerTests()
        self.provisioner, *_ = self.fixture.make(Path(self.directory.name).resolve())
        self.service = DirectSandboxService(self.provisioner, max_concurrent_restores=1)
        self.service.admission_wait_seconds = 3

    def parked(self):
        self.fixture.create(self.service, self.fixture.spec())
        self.service.park("sandbox", operation_id="park:test")

    def configure_memory(self):
        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=8192),
            runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_count=4,
                cpu_percent=10,
                memory_total_mb=8192,
                memory_available_mb=8192,
            ),
        )

    def assert_clean(self):
        self.assertEqual(self.service.activity_snapshot().active_operations, 0)
        for item in self.service.transition_admission_snapshot().values():
            self.assertEqual(item["active"], 0)
            self.assertEqual(item["waiting"], 0)
        self.assertEqual(self.service._locks, {})

    def test_disk_overlap_rejection_is_retryable_only_while_checkpoint_stays_parked(self):
        self.parked()
        with patch.object(self.service.warden, "resume",
                          side_effect=DirectRegistryCapacityUnavailable("disk overlap queued")):
            with self.assertRaises(SandboxRestoreBusyError):
                self.service.wake("sandbox", generation=7, operation_id="wake:overlap")
        self.assertEqual(self.service.get("sandbox").state, "parked")
        self.assert_clean()
        self.assertEqual(self.service.wake("sandbox", generation=7,
            operation_id="wake:overlap").state, "running")

    def test_post_candidate_disk_error_is_not_reclassified_as_safe_retry(self):
        self.parked()
        registration = self.provisioner.registry.get("sandbox")
        sandbox = registration.to_direct_sandbox()

        def fail_after_candidate(*_, **__):
            self.service.warden.records[self.service.warden.key(sandbox)] = SimpleNamespace(
                state=HibernationState.RESTORING, hibernation_generation=1)
            raise DirectRegistryCapacityUnavailable("ambiguous candidate error")

        with patch.object(self.service.warden, "resume", side_effect=fail_after_candidate):
            with self.assertRaises(DirectRegistryCapacityUnavailable):
                self.service.wake("sandbox", generation=7, operation_id="wake:overlap")
        self.assert_clean()

    def test_startup_cannot_steal_contended_restore_headroom(self):
        self.configure_memory()  # 6 GiB usable above the unchanged 2 GiB floor.
        entered = Event()
        request = ResourceQuantity(memory_mb=4096)

        def start():
            with self.service._reserve_active_capacity("new", 1, request):
                entered.set()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.service._transition_demand(
                ("wake", 1), TransitionCost(TransitionKind.RESTORE, 4 << 30)
            ):
                future = pool.submit(start)
                self.assertFalse(entered.wait(.3))
                self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
                # Restore itself can spend its protected headroom immediately.
                with self.service._reserve_active_capacity(
                    "wake", 1, request,
                    cost=TransitionCost(TransitionKind.RESTORE, 4 << 30),
                ):
                    self.assertFalse(entered.is_set())
            future.result(2)  # Cancellation/completion removes the protection.
        self.assertTrue(entered.is_set())
        self.assert_clean()

    def test_restore_protection_does_not_serialize_work_that_fits(self):
        self.configure_memory()
        with self.service._transition_demand(
            ("wake", 1), TransitionCost(TransitionKind.RESTORE, 2 << 30)
        ):
            with self.service._reserve_active_capacity(
                "new", 1, ResourceQuantity(memory_mb=2048),
            ):
                with self.service._reserve_active_capacity(
                    "wake", 1, ResourceQuantity(memory_mb=2048),
                    cost=TransitionCost(TransitionKind.RESTORE, 2 << 30),
                ):
                    self.assertEqual(self.service._transitions.known_memory_bytes, 4 << 30)
        self.assert_clean()

    def test_file_restore_reserves_physical_bound_without_spending_tmpfs(self):
        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=16384),
            runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_count=4, cpu_percent=0,
                memory_total_mb=16384, memory_available_mb=12288,
                memory_backing=MemoryBackingCapacity(8 << 30, 0, 'ram-mount'),
            ),
        )
        with patch.object(self.service.warden, 'application_memory_mode', return_value='file'):
            cost = self.service._transition_memory_cost(TransitionKind.RESTORE, ('file', 1), 4 << 30)
            self.assertEqual(cost.ram_backing_bytes, 0)
            with self.service._reserve_active_capacity(
                'file', 1, ResourceQuantity(memory_mb=4096), cost=cost,
            ):
                demand = self.service.warm_park_demand()
                self.assertEqual(demand.physical_bytes, 4 << 30)
                self.assertEqual(demand.ram_backing_bytes, 0)
        self.assert_clean()

    def test_physical_memory_wait_retains_demand_until_resident_reclaim(self):
        available = [3072]
        sampled = Event()
        def metrics():
            sampled.set()
            return NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_count=4, cpu_percent=10,
                memory_total_mb=8192, memory_available_mb=available[0],
            )
        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=8192), runtime_metrics_provider=metrics,
        )
        pressure = SimpleNamespace(memory_available_bytes=3072 * 1024**2,
                                   memory_fraction=3072 / 8192,
                                   io_stall=0, memory_stall=0)
        self.assertFalse(decide_resident_wait(pressure, MemoryDemand(0, 0)).reclaim)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.fixture.create, self.service,
                                 replace(self.fixture.spec(), memory_mb=4096))
            self.assertTrue(sampled.wait(1))
            # The node's resident scheduler wakes every 250ms. The demand must
            # survive that interval rather than disappearing with an HTTP 503.
            self.assertFalse(Event().wait(0.3))
            self.assertFalse(future.done())
            incoming = self.service.warm_park_demand().physical_bytes
            self.assertEqual(incoming, 4096 * 1024**2)
            decision = decide_resident_wait(pressure, MemoryDemand(incoming, incoming))
            self.assertTrue(decision.reclaim)
            self.assertEqual(decision.reason, "queued_demand")
            available[0] = 6144  # physical sample after resident capture/reap
            self.assertEqual(future.result(2).state, "running")
        self.assert_clean()

    def test_physical_memory_wait_has_operation_deadline(self):
        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=8192),
            runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_count=4, cpu_percent=10,
                memory_total_mb=8192, memory_available_mb=3072,
            ),
        )
        self.service.admission_wait_seconds = 0.4
        with self.assertRaises(SandboxCapacityUnavailableError):
            self.fixture.create(self.service, replace(self.fixture.spec(), memory_mb=4096))
        self.assert_clean()

    def test_first_start_preserves_physical_floor_even_with_free_swap(self):
        available, sampled = [5120], Event()

        def metrics():
            sampled.set()
            return NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_count=4, cpu_percent=10,
                memory_total_mb=8192, memory_available_mb=available[0],
                swap_total_mb=65536, swap_free_mb=65536,
            )

        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=8192), runtime_metrics_provider=metrics,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.fixture.create, self.service,
                                 replace(self.fixture.spec(), memory_mb=4096))
            self.assertTrue(sampled.wait(1))
            self.assertFalse(Event().wait(0.3))
            self.assertFalse(future.done())
            self.assertEqual(self.service._transitions.known_memory_bytes, 0)
            self.assertEqual(self.service.warm_park_demand().physical_bytes, 4096 * 1024**2)
            available[0] = 6144
            self.assertEqual(future.result(2).state, "running")
        self.assert_clean()

    def test_configured_ram_capacity_queues_until_verified_space_returns(self):
        for initial in (MemoryBackingCapacity(), MemoryBackingCapacity(8 << 30, 1 << 30, "mount")):
            with self.subTest(backing=initial):
                evidence, entered, sampled = [initial], Event(), Event()

                def metrics():
                    sampled.set()
                    return NodeRuntimeMetrics(
                        collected_at=utc_now(), cpu_count=4, cpu_percent=10,
                        memory_total_mb=16384, memory_available_mb=12288,
                        swap_total_mb=65536, swap_free_mb=65536,
                        memory_backing=evidence[0],
                    )

                self.service.configure_active_capacity(
                    ResourceQuantity(vcpu=4, memory_mb=16384), runtime_metrics_provider=metrics,
                )

                def start():
                    with self.service._startup_demand("queued", 1, ResourceQuantity(memory_mb=2048)):
                        with self.service._reserve_active_capacity("queued", 1, ResourceQuantity(memory_mb=2048)):
                            entered.set()

                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(start)
                    self.assertTrue(sampled.wait(1))
                    self.assertFalse(entered.wait(0.3))
                    self.assertEqual(self.service.warm_park_demand().physical_bytes, 2 << 30)
                    evidence[0] = MemoryBackingCapacity(8 << 30, 2 << 30, "mount")
                    future.result(2)
                self.assertTrue(entered.is_set())
                self.assert_clean()

    def test_unknown_physical_memory_cannot_bypass_configured_backing(self):
        for backing in (None, MemoryBackingCapacity(), MemoryBackingCapacity(8 << 30, 8 << 30, "mount")):
            with self.subTest(backing=backing):
                self.service.configure_active_capacity(
                    ResourceQuantity(vcpu=4, memory_mb=8192),
                    runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                        collected_at=utc_now(), cpu_count=4, cpu_percent=0,
                        memory_total_mb=0, memory_available_mb=6144,
                        memory_backing=backing,
                    ),
                )
                with patch.object(self.service.warden.config, "application_memory_root", Path('/ram'), create=True):
                    with self.assertRaises(SandboxCapacityUnavailableError):
                        with self.service._reserve_active_capacity(
                            "unknown", 1, ResourceQuantity(memory_mb=1024),
                            deadline=__import__('time').monotonic() + 0.25,
                        ):
                            self.fail("unknown physical capacity admitted a growth transition")
                self.assert_clean()

    def test_concurrent_starts_cannot_spend_same_memory_headroom(self):
        self.configure_memory()
        entered = Event()
        request = ResourceQuantity(vcpu=1, memory_mb=4096)

        def second():
            with self.service._reserve_active_capacity("second", 1, request):
                entered.set()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.service._reserve_active_capacity("first", 1, request):
                future = pool.submit(second)
                self.assertFalse(entered.wait(0.1))
                self.assertEqual(
                    self.service._transitions.known_memory_bytes, 4096 * 1024**2
                )
            future.result(2)
            self.assertTrue(entered.is_set())
        self.assert_clean()

    def test_queued_wake_delete_and_drain_release_slots_and_lifecycle_lock(self):
        for action in ("delete", "drain"):
            with self.subTest(action=action):
                if action == "drain":
                    self.setUp()
                self.parked()
                self.service._restore_slots.acquire()
                with (
                    ThreadPoolExecutor(max_workers=1) as pool,
                    patch.object(self.service.warden, "resume") as resume,
                ):
                    future = pool.submit(
                        self.service.wake,
                        "sandbox",
                        generation=7,
                        operation_id="wake:queued",
                    )
                    try:
                        wait_queued(self.service._restore_slots, 1)
                        if action == "delete":
                            self.service.delete("sandbox", generation=7)
                        else:
                            self.service.close_admission()
                        with self.assertRaises(SandboxRestoreBusyError):
                            future.result(1)
                        resume.assert_not_called()
                    finally:
                        self.service._restore_slots.release()
                self.assert_clean()
                if action == "drain":
                    self.service.delete("sandbox", generation=7)

    def test_delete_interrupts_memory_wait_after_slot_acquired(self):
        self.parked()
        self.configure_memory()
        sampled = Event()
        original = self.service._runtime_metrics_provider

        def metrics():
            sampled.set()
            return original()

        with self.service._reserve_active_capacity(
            "busy-start", 1, ResourceQuantity(vcpu=1, memory_mb=6144)
        ):
            self.service._runtime_metrics_provider = metrics
            with (
                ThreadPoolExecutor(max_workers=1) as pool,
                patch.object(self.service.warden, "resume") as resume,
            ):
                future = pool.submit(
                    self.service.wake,
                    "sandbox",
                    generation=7,
                    operation_id="wake:memory",
                )
                self.assertTrue(sampled.wait(1))
                self.service.delete("sandbox", generation=7)
                with self.assertRaises(DirectWardenError):
                    future.result(1)
                resume.assert_not_called()
        self.assert_clean()

    def test_delete_interrupts_physical_memory_wait(self):
        self.parked()
        sampled = Event()
        def metrics():
            sampled.set()
            return NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_count=4, cpu_percent=10,
                memory_total_mb=8192, memory_available_mb=1024,
            )
        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=8192), runtime_metrics_provider=metrics,
        )
        with (ThreadPoolExecutor(max_workers=1) as pool,
              patch.object(self.service.warden, "resume") as resume):
            future = pool.submit(self.service.wake, "sandbox", generation=7,
                                 operation_id="wake:physical-memory")
            self.assertTrue(sampled.wait(1))
            self.service.delete("sandbox", generation=7)
            with self.assertRaises(DirectWardenError):
                future.result(1)
            resume.assert_not_called()
        self.assert_clean()

    def test_two_queued_wakes_restore_checkpoint_once(self):
        self.parked()
        self.service._restore_slots.acquire()
        with (
            ThreadPoolExecutor(max_workers=2) as pool,
            patch.object(
                self.service.warden, "resume", wraps=self.service.warden.resume
            ) as resume,
        ):
            first = pool.submit(
                self.service.wake, "sandbox", generation=7, operation_id="wake:first"
            )
            wait_queued(self.service._restore_slots, 1)
            second = pool.submit(
                self.service.wake, "sandbox", generation=7, operation_id="wake:second"
            )
            wait_queued(self.service._restore_slots, 2)
            self.service._restore_slots.release()
            self.assertEqual(first.result(2).state, "running")
            self.assertEqual(second.result(2).state, "running")
            self.assertEqual(resume.call_count, 1)
        self.assert_clean()

    def test_capture_observation_releases_after_native_failure(self):
        self.fixture.create(self.service, self.fixture.spec())

        def fail(*args, **kwargs):
            snapshot = self.service.transition_admission_snapshot()["capture"]
            self.assertEqual(snapshot["active"], 1)
            self.assertIsNone(snapshot["memory_bytes"])
            raise OSError("capture failed")

        with patch.object(self.service.warden, "park", side_effect=fail):
            with self.assertRaisesRegex(OSError, "capture failed"):
                self.service.park("sandbox", operation_id="park:fail")
        self.assert_clean()

    def test_restore_cost_uses_authenticated_allocations_not_quota(self):
        self._exercise_serialized_restore_cost(version=2)

    def test_split_restore_cost_and_wake_use_actual_nested_artifact_metadata(self):
        self._exercise_serialized_restore_cost(version=3)

    def test_split_checkpoint_wake_waits_for_physical_memory(self):
        self._exercise_serialized_restore_cost(version=3, physical_pressure=True)

    def test_resident_capture_progresses_while_split_restore_owns_last_queue_slot(self):
        self._exercise_serialized_restore_cost(
            version=3, physical_pressure=True, reclaim_capture=True,
        )

    def _exercise_serialized_restore_cost(
        self, *, version, physical_pressure=False, reclaim_capture=False,
    ):
        self.parked()
        if reclaim_capture:
            self.fixture.create(
                self.service, replace(self.fixture.spec(), id="resident", memory_mb=2048),
            )
        store = self.service.warden.artifacts
        # The actual publisher serializes nested LocalHibernationArtifactFile
        # values. A namespace with allocated_bytes directly on the file hid a
        # production restore failure and is deliberately not used here.
        store.root.chmod(0o700)
        generation = store.prepare_generation(
            sandbox_id="sandbox", sandbox_generation=7, hibernation_generation=1,
        )
        files = []
        for name, role in (
            ("application_memory.img", HibernationFileRole.MAIN_MEMORY),
            ("checkpoint.img", HibernationFileRole.KERNEL_STATE),
            ("pages_meta.img", HibernationFileRole.ALLOCATOR_METADATA),
        ):
            path = generation / name
            with path.open("wb") as handle:
                handle.write(b"x" * 4096)
                if role == HibernationFileRole.MAIN_MEMORY:
                    handle.truncate(8 * 1024**2)
            files.append(LocalHibernationArtifactFile.from_path(path, role=role))
        registration = self.provisioner.registry.get("sandbox")
        manifest = HibernationManifest(
            sandbox_id="sandbox", sandbox_generation=7, hibernation_generation=1,
            operation_id="park:test", spec_sha256=registration.spec_sha256,
            container_id=registration.container_id, created_ns=1,
            runtime=self.service.warden.config.runtime_fingerprint, files=tuple(files),
            managed_process_sha256="", version=version,
            workspace=WorkspaceCaptureRef("workspace-sandbox.sandbox-7", "a" * 64) if version == 3 else None,
            memory=MemoryBackingRef("sandbox.sandbox-7", 1 << 30) if version == 3 else None,
        )
        store.publish_complete(manifest)
        expected = sum(path.stat().st_blocks * 512 for path in
                       (generation / file.artifact.name for file in files))
        with patch.object(
            store, "load_published_metadata", wraps=store.load_published_metadata,
        ) as metadata:
            cost = self.service._restore_cost(
                "sandbox", 7, ResourceQuantity(memory_mb=8192)
            )
        self.assertEqual(cost.memory_bytes, expected)
        self.assertEqual(cost.read_bytes, cost.memory_bytes)
        self.assertEqual(cost.provenance, "authenticated-checkpoint-allocation")
        metadata.assert_called_once_with(
            sandbox_id="sandbox", sandbox_generation=7, hibernation_generation=1
        )
        self.configure_memory()
        original = self.service.warden.resume

        def resume(*args, **kwargs):
            active = self.service.transition_admission_snapshot()["restore"]
            self.assertEqual(active["active"], 1)
            self.assertEqual(active["memory_bytes"], expected)
            self.assertEqual(active["read_bytes"], expected)
            return original(*args, **kwargs)

        with patch.object(self.service.warden, "resume", side_effect=resume) as resumed:
            if physical_pressure:
                metrics_provider = self.service._runtime_metrics_provider
                available, sampled = [1024], Event()
                def pressured_metrics():
                    sampled.set()
                    return replace(metrics_provider(), memory_available_mb=available[0])
                self.service._runtime_metrics_provider = pressured_metrics
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(self.service.wake, "sandbox", generation=7,
                                         operation_id="wake:serialized-cost")
                    self.assertTrue(sampled.wait(1))
                    self.assertFalse(Event().wait(0.3))
                    self.assertFalse(future.done())
                    resumed.assert_not_called()
                    self.assertEqual(self.service.warm_park_demand().physical_bytes, expected)
                    if reclaim_capture:
                        policy = WarmParkPolicy(
                            lambda: Pressure(
                                memory_fraction=available[0] / 8192,
                                memory_available_bytes=available[0] * 1024**2,
                            ), demand=self.service.warm_park_demand,
                        )
                        original_park = self.service.warden.park

                        def release_resident(*args, **kwargs):
                            snapshot = self.service.transition_admission_snapshot()
                            self.assertEqual(snapshot["restore"]["waiting"], 1)
                            self.assertEqual(snapshot["restore"]["active"], 0)
                            self.assertEqual(snapshot["capture"]["active"], 1)
                            record = original_park(*args, **kwargs)
                            # The only capacity change comes after the existing
                            # capture/reap path finishes, not from a policy wish.
                            available[0] = 4096
                            return record

                        key = ("resident", 7, "relay-wait")
                        with policy.defer(key, memory_bytes=1536 * 1024**2, blocking=False):
                            with patch.object(self.service.warden, "park", side_effect=release_resident):
                                self.assertEqual(self.service.park(
                                    "resident", operation_id="park:pressure-reclaim",
                                ).state, "parked")
                            policy.parked(key)
                        self.assertEqual(policy.snapshot()["checkpoints_completed"], 1)
                    else:
                        available[0] = 8192
                    self.assertEqual(future.result(2).state, "running")
            else:
                self.assertEqual(self.service.wake(
                    "sandbox", generation=7, operation_id="wake:serialized-cost").state, "running")
        self.assert_clean()

    def test_changed_checkpoint_is_requoted_before_restore(self):
        self.parked()
        self.service._restore_slots.acquire()
        with (
            ThreadPoolExecutor(max_workers=1) as pool,
            patch.object(self.service.warden, "resume") as resume,
        ):
            future = pool.submit(
                self.service.wake, "sandbox", generation=7, operation_id="wake:old-cost"
            )
            try:
                wait_queued(self.service._restore_slots, 1)
                with self.service._lock("sandbox", 7):
                    old = self.service.warden.records[("sandbox", 7)]
                    self.service.warden.records[("sandbox", 7)] = SimpleNamespace(
                        state=old.state, hibernation_generation=2
                    )
            finally:
                self.service._restore_slots.release()
            with self.assertRaisesRegex(SandboxRestoreBusyError, "checkpoint changed"):
                future.result(1)
            resume.assert_not_called()
        self.assert_clean()

    def test_foreground_burst_preserves_one_publication_lane(self):
        self.parked()
        self.fixture.create(self.service, replace(self.fixture.spec(), id="second"))
        self.service.park("second", operation_id="park:second")
        entered, release = Event(), Event()

        def publish(*args, **kwargs):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("test publisher gate")

        with (
            patch.object(
                self.service.warden,
                "publish_storage_snapshot",
                side_effect=publish,
                create=True,
            ),
            patch.object(self.service, "describe_storage_native_snapshot"),
        ):
            with self.service._startup_demand(
                "queued", 1, ResourceQuantity(memory_mb=1024)
            ):
                self.service.request_storage_publication("sandbox", generation=7)
                self.assertTrue(entered.wait(1))
                self.service.request_storage_publication("second", generation=7)
                self.assertNotIn(("second", 7), self.service._publication_threads)
                snapshot = self.service.transition_admission_snapshot()["publication"]
                self.assertEqual(snapshot["active"], 1)
                self.assertIsNone(snapshot["memory_bytes"])
                release.set()
                self.service._publication_threads[("sandbox", 7)].join(2)
                self.service.request_storage_publication("second", generation=7)
                self.service._publication_threads[("second", 7)].join(2)
        self.assert_clean()
