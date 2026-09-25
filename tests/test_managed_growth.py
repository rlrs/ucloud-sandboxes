"""Assembled managed admission keeps future heap growth visible until a safe wait."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from dataclasses import replace
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Event
import time
import unittest
from unittest.mock import patch

from tests import test_direct_provisioner as fixtures
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry, DirectRegistryConflictError
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.managed_process import ManagedProcessStart
from ucloud_sandboxes.hibernation import HibernationManifest, HibernationFileRole, LocalHibernationArtifactFile
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.resident_memory import ResidentMemorySample
from ucloud_sandboxes.sandbox import SandboxAdmissionClosedError, SandboxStartupBusyError


class ManagedGrowthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fixture = fixtures.DirectProvisionerTests()
        self.provisioner, self.registry, *_ = self.fixture.make(Path(self.tmp.name).resolve())
        self.provisioner.oci = replace(self.provisioner.oci, managed_init_binary=Path('/bin/sh').resolve())
        self.service = DirectSandboxService(self.provisioner)
        self.service.admission_wait_seconds = 3
        for sid in ('one', 'two'):
            self.fixture.create(self.service, replace(self.fixture.spec(), id=sid, memory_mb=4096,
                                                     parkable=True, managed_process=True))
        self.available = 6144
        self.configure(self.service)
        self.control = patch.object(self.service, '_managed_control', side_effect=self.response).start()
        self.addCleanup(patch.stopall)
        self.spec = ManagedProcessStart('primary', ('/bin/agent',), cwd='/workspace')

    def configure(self, service):
        service.configure_active_capacity(ResourceQuantity(vcpu=4, memory_mb=8192),
            runtime_metrics_provider=lambda: NodeRuntimeMetrics(collected_at=utc_now(), cpu_count=4,
                cpu_percent=0, memory_total_mb=8192, memory_available_mb=self.available))

    @staticmethod
    def response(registration, payload, **_):
        return {'version': 1, 'ok': True, 'job': {'job_id': payload['job_id'], 'spec_sha256': 'a'*64,
                                    'state': 'running', 'pid': 10, 'sequence': 1}}

    def wait_for_demand(self, service=None):
        service = service or self.service
        deadline = time.monotonic() + 1
        while not service._transitions.foreground_waiting and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertTrue(service._transitions.foreground_waiting)

    def test_resident_continuation_does_not_require_a_restore_slot(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        with ThreadPoolExecutor(max_workers=1) as pool:
            with ExitStack() as held:
                for index in range(self.service._restore_slots.capacity):
                    held.enter_context(self.service._restore_slot(owner=(f'restore-{index}', 1)))
                wake = pool.submit(self.service.admit_managed_continuation,
                                   'one', 7, 'request-one')
                wake.result(1)
                self.assertEqual(self.service._restore_slots.waiting, 0)
                self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))
                self.assertEqual(self.service.get('one').state, 'running')

    def test_memory_blocked_continuation_does_not_occupy_restore_slot(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        self.service.start_managed_process('two', self.spec)
        with ThreadPoolExecutor(max_workers=1) as pool:
            wake = pool.submit(self.service.admit_managed_continuation,
                               'one', 7, 'request-one')
            try:
                self.wait_for_demand()
                self.assertFalse(wake.done())
                self.assertFalse(self.registry.relay_wake_fence('one', 7, 'request-one'))
                # Every I/O permit remains usable while growth waits for RAM.
                with ExitStack() as held:
                    for index in range(self.service._restore_slots.capacity):
                        held.enter_context(self.service._restore_slot(
                            owner=(f'restore-{index}', 1), deadline=time.monotonic() + .5))
            finally:
                self.service.observe_managed_wait('two', 7, 'request-two')
            wake.result(2)
            self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))

    def test_duplicate_continuations_share_one_growth_reservation(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        with ThreadPoolExecutor(max_workers=8) as pool:
            wakes = [pool.submit(self.service.admit_managed_continuation,
                                 'one', 7, 'request-one') for _ in range(8)]
            for wake in wakes:
                wake.result(2)
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
        self.assertFalse(self.service._transitions.foreground_waiting)
        self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))

    def test_growth_commits_do_not_hold_node_capacity_guard(self):
        """Every exec/upload admission shares the guard; fsyncs must not."""
        observed = []
        original = self.registry.growth_intent

        def committing(*args, **kwargs):
            observed.append((kwargs['action'], self.service._capacity_guard.locked()))
            return original(*args, **kwargs)

        with patch.object(self.registry, 'growth_intent', side_effect=committing):
            self.service.start_managed_process('one', self.spec)
            self.service.observe_managed_wait('one', 7, 'request-one')
            self.service.admit_managed_continuation('one', 7, 'request-one')
        # launch, startup admission, safe wait, continuation admission
        self.assertEqual([action for action, _ in observed], ['launch', 'activate', 'wait', 'activate'])
        self.assertEqual([held for _, held in observed], [False] * 4)
        self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def test_admitted_growth_stays_charged_while_its_commit_is_in_flight(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        self.service.start_managed_process('two', self.spec)
        self.service.observe_managed_wait('two', 7, 'request-two')
        committing, release = Event(), Event()
        original = self.registry.growth_intent

        def slow_activate(*args, **kwargs):
            if kwargs['action'] == 'activate' and args[0] == 'one':
                committing.set()
                self.assertTrue(release.wait(5))
            return original(*args, **kwargs)

        with patch.object(self.registry, 'growth_intent', side_effect=slow_activate), \
                ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.service.admit_managed_continuation, 'one', 7, 'request-one')
            self.assertTrue(committing.wait(2))
            # The guard is free during the commit, yet the headroom is spent:
            # only one 4 GiB continuation fits in 6 GiB available.
            self.assertFalse(self.service._capacity_guard.locked())
            self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
            second = pool.submit(self.service.admit_managed_continuation, 'two', 7, 'request-two')
            self.wait_for_demand()
            self.assertFalse(second.done())
            release.set()
            first.result(2)
            self.assertFalse(second.done())
            self.service.observe_managed_wait('one', 7, 'request-one-next')
            second.result(2)
        self.assertEqual(self.service._provisional_growth, {})
        self.assertEqual(self.service._growth_turns, {})

    def test_wait_winning_before_covered_check_requires_growth_admission(self):
        self.service.start_managed_process('one', self.spec)
        original = self.service._growth_turn
        intercepted = False

        @contextmanager
        def wait_first(key):
            nonlocal intercepted
            if not intercepted:
                intercepted = True
                # Simulate the wait taking the same-sandbox turn just before
                # this wake. It must not use an earlier "active" observation.
                self.service.observe_managed_wait('one', 7, 'request-race')
            with original(key):
                yield

        with patch.object(self.service, '_growth_turn', side_effect=wait_first):
            self.service.admit_managed_continuation('one', 7, 'request-race')
        self.assertTrue(intercepted)
        self.assertEqual(self.service._growth_intents[('one', 7)].phase, 'active')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
        self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-race'))

    def test_existing_wake_fence_does_not_take_registry_writer(self):
        self.service.start_managed_process('one', self.spec)
        self.registry.relay_wake_fence('one', 7, 'request-one', record=True)
        with patch.object(self.registry, '_transaction', wraps=self.registry._transaction) as tx:
            self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one', record=True))
        self.assertEqual([call.kwargs['write'] for call in tx.call_args_list], [False])
        with patch.object(self.registry, '_transaction', wraps=self.registry._transaction) as tx:
            self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-two', record=True))
        self.assertEqual([call.kwargs['write'] for call in tx.call_args_list], [False, True])

    def test_managed_burst_waits_after_runtime_create_until_first_safe_wait(self):
        self.service.start_managed_process('one', self.spec)
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
        with ThreadPoolExecutor(max_workers=1) as pool:
            second = pool.submit(self.service.start_managed_process, 'two', self.spec)
            self.wait_for_demand()
            self.assertFalse(second.done())
            self.assertEqual(self.control.call_count, 1)
            self.service.observe_managed_wait('one', 7, 'request-one')
            self.assertEqual(second.result(2).job_id, 'primary')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def test_response_growth_burst_settles_without_parking_or_overadmission(self):
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkDeferred, WarmParkPolicy

        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        self.service.start_managed_process('two', self.spec)
        runtime = DirectNodeRuntime(self.service)
        runtime._warm_parks = WarmParkPolicy(
            lambda: Pressure(.75, 0, 0, 6 << 30),
            demand=self.service.warm_park_demand,
        )
        with self.assertRaises(WarmParkDeferred):
            runtime.park_with_activity_revision('one', generation=7,
                operation_id='park:one', relay_request_id='request-one')
        with patch.object(self.service, 'park', wraps=self.service.park) as park, \
                ThreadPoolExecutor(max_workers=1) as pool:
            wake = pool.submit(runtime.wake_with_activity_revision, 'one', generation=7,
                               operation_id='wake:one', relay_request_id='request-one')
            self.wait_for_demand()
            self.assertFalse(wake.done())
            self.assertEqual(self.service.warm_park_demand().physical_bytes, 8 << 30)
            with self.assertRaises(WarmParkDeferred):
                runtime.park_with_activity_revision('one', generation=7,
                    operation_id='park:one', relay_request_id='request-one')
            # The active agent reaches its next safe wait. Admission transfers
            # its growth headroom to the queued response without checkpoint I/O.
            self.service.observe_managed_wait('two', 7, 'request-two')
            record, _ = wake.result(2)
            self.assertEqual(record.state, 'running')
            self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
            park.assert_not_called()
            self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))

    def test_ambiguous_launch_rehydrates_and_changed_job_never_replaces_it(self):
        self.control.side_effect = TimeoutError('ambiguous control RPC')
        with self.assertRaises(TimeoutError):
            self.service.start_managed_process('one', self.spec)
        restarted = DirectSandboxService(self.provisioner)
        self.configure(restarted)
        self.assertEqual(restarted.warm_park_demand().physical_bytes, 4 << 30)
        with self.assertRaises(DirectRegistryConflictError):
            restarted.start_managed_process('one', replace(self.spec, job_id='different'))
        self.assertEqual(self.registry.growth_intents()[0].phase, 'active')

    def test_running_continuation_queues_without_holding_runtime_lifecycle(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        self.service.start_managed_process('two', self.spec)
        runtime = DirectNodeRuntime(self.service)
        with ThreadPoolExecutor(max_workers=1) as pool:
            wake = pool.submit(runtime.wake_with_activity_revision, 'one', generation=7,
                               operation_id='wake:one', relay_request_id='request-one')
            self.wait_for_demand()
            self.assertFalse(wake.done())
            self.assertTrue(runtime.lifecycle.is_idle('one'))
            self.assertFalse(self.registry.relay_wake_fence('one', 7, 'request-one'))
            self.service.park('one', operation_id='pressure:queued-continuation')
            self.assertEqual(self.service.get('one').state, 'parked')
            # A different live waiter can relinquish its exposure and be parked;
            # the continuation owns neither an exclusive lifecycle nor a slot
            # that capture needs to make progress.
            self.service.observe_managed_wait('two', 7, 'request-two')
            record, _ = wake.result(2)
            self.assertEqual(record.state, 'running')
        self.assertEqual(self.registry.growth_intents()[0].phase, 'active')
        with self.assertRaisesRegex(DirectRegistryConflictError, 'superseded'):
            self.service.observe_managed_wait('one', 7, 'request-one')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def test_residual_forecast_credits_only_fresh_incarnation_footprint(self):
        self.service.start_managed_process('one', self.spec)
        sample = ResidentMemorySample(current_bytes=3 << 30, anonymous_bytes=0, file_bytes=3 << 30,
            dirty_bytes=0, writeback_bytes=0, refault_file_pages=0, cgroup_path='/owned',
            cgroup_device=1, cgroup_inode=2, sentry_pid=10, sentry_start_time_ticks=20,
            sampled_at=time.monotonic(), shared_memory_bytes=2 << 30)
        self.service._resident_memory._samples[('one', 7)] = sample
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 2 << 30)
        self.service._resident_memory._samples[('one', 7)] = replace(sample, sampled_at=0)
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
        self.service._resident_memory._samples[('one', 8)] = sample
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def sample(self, *, current, peak=0, shared=None, age=0.0):
        return ResidentMemorySample(current_bytes=current, anonymous_bytes=0, file_bytes=current,
            dirty_bytes=0, writeback_bytes=0, refault_file_pages=0, cgroup_path='/owned',
            cgroup_device=1, cgroup_inode=2, sentry_pid=10, sentry_start_time_ticks=20,
            sampled_at=time.monotonic() - age,
            shared_memory_bytes=current if shared is None else shared, peak_bytes=peak)

    def test_continuation_forecasts_physical_growth_to_its_demonstrated_peak(self):
        self.available = 8192  # room for the second launch's whole bound
        with patch.object(self.service.warden, 'application_memory_mode', return_value='ram'):
            self.service.start_managed_process('one', self.spec)
            self.service.observe_managed_wait('one', 7, 'wait-1')
            self.service.admit_managed_continuation('one', 7, 'wait-1')
            self.service._resident_memory._samples[('one', 7)] = self.sample(
                current=1 << 30, peak=3 << 29)
            demand = self.service.warm_park_demand()
            # Physical: back to the 1.5 GiB peak. Unswappable RAM backing keeps
            # the whole 4 GiB bound, where overshoot would be SIGBUS.
            self.assertEqual(demand.physical_bytes, 1 << 29)
            self.assertEqual(demand.ram_backing_bytes, 3 << 30)
            # A launch has no safe wait yet and keeps its whole bound.
            self.service.start_managed_process('two', self.spec)
            self.service._resident_memory._samples[('two', 7)] = self.sample(
                current=1 << 30, peak=3 << 29)
            demand = self.service.warm_park_demand()
            self.assertEqual(demand.physical_bytes, (1 << 29) + (3 << 30))

    def test_growth_credit_survives_a_slow_refresh_pass(self):
        self.service.start_managed_process('one', self.spec)
        self.service._resident_memory._samples[('one', 7)] = self.sample(current=3 << 30, age=10)
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 1 << 30)
        self.service._resident_memory._samples[('one', 7)] = self.sample(current=3 << 30, age=40)
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def test_terminal_and_delete_release_only_matching_primary_generation(self):
        record = self.service.start_managed_process('one', self.spec)
        self.service._observe_managed_terminal(replace(record, state='exited', job_id='old-job'))
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
        with self.assertRaises(DirectRegistryConflictError):
            self.service._observe_managed_terminal(replace(record, state='exited', sandbox_generation=6))
        self.service._observe_managed_terminal(replace(record, state='exited'))
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 0)
        with self.assertRaises(DirectRegistryConflictError):
            self.service.start_managed_process('one', replace(self.spec, argv=('different',)))
        self.service.delete('one', generation=7)
        self.assertEqual(self.registry.growth_intents(), ())

    def test_file_growth_does_not_reappear_when_reclaimable_pages_are_evicted(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'file-wait')
        with patch.object(self.service.warden, 'application_memory_mode', return_value='file'):
            self.service.admit_managed_continuation('one', 7, 'file-wait')
            for resident in (3 << 30, 64 << 20, 0):
                self.service._resident_memory._samples[('one', 7)] = ResidentMemorySample(
                    current_bytes=resident, anonymous_bytes=0, file_bytes=resident,
                    dirty_bytes=0, writeback_bytes=0, refault_file_pages=0,
                    cgroup_path='/owned', cgroup_device=1, cgroup_inode=2,
                    sentry_pid=10, sentry_start_time_ticks=20, sampled_at=time.monotonic())
                demand = self.service.warm_park_demand()
                self.assertEqual((demand.physical_bytes, demand.ram_backing_bytes), (0, 0))
                self.assertEqual(self.service.resident_demand_snapshot()['unknown_transition_memory_costs'], 1)
            # This describes reclaimable growth, not a zero-cost native restore.
            cost = self.service._restore_cost('one', 7, self.registry.get('one').spec.requested_resources())
            self.assertEqual(cost.memory_bytes, 4 << 30)
            self.assertEqual(cost.ram_backing_bytes, 0)

    def test_file_continuation_cannot_spend_physical_headroom_promised_to_ram(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'file-wait')
        self.service.start_managed_process('two', self.spec)
        self.available = 5120  # 3 GiB usable, less than the other RAM promise.
        with patch.object(self.service.warden, 'application_memory_mode',
                          side_effect=lambda sid, generation: 'file' if sid == 'one' else 'ram'):
            with ThreadPoolExecutor(max_workers=1) as pool:
                wake = pool.submit(self.service.admit_managed_continuation, 'one', 7, 'file-wait')
                self.wait_for_demand()
                self.assertFalse(wake.done())
                self.assertFalse(self.registry.relay_wake_fence('one', 7, 'file-wait'))
                self.service.observe_managed_wait('two', 7, 'ram-wait')
                wake.result(2)
        self.assertTrue(self.registry.relay_wake_fence('one', 7, 'file-wait'))

    def test_drain_cancels_undispatched_growth_without_forgetting_active_launch(self):
        self.service.start_managed_process('one', self.spec)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.service.start_managed_process, 'two', self.spec)
            self.wait_for_demand()
            self.service.close_admission()
            with self.assertRaises(SandboxAdmissionClosedError):
                pending.result(2)
        self.assertEqual(self.control.call_count, 1)
        self.assertEqual([item.phase for item in self.registry.growth_intents()], ['active', 'queued'])
        self.assertFalse(self.service._transitions.foreground_waiting)

    def test_forced_capture_releases_growth_and_restore_reestablishes_it(self):
        self.service.start_managed_process('one', self.spec)
        self.service.park('one', operation_id='forced:one')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 0)
        self.service.wake('one', generation=7, operation_id='wake:one')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def test_granted_continuation_atomically_revokes_late_safe_wait(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        self.service.admit_managed_continuation('one', 7, 'request-one')
        self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))
        with self.assertRaises(DirectRegistryConflictError):
            self.service.observe_managed_wait('one', 7, 'request-one')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def test_delete_cancels_pending_launch_before_dispatch(self):
        self.service.start_managed_process('one', self.spec)
        runtime = DirectNodeRuntime(self.service)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(runtime.start_managed_process, 'two', self.spec)
            self.wait_for_demand()
            runtime.delete('two', generation=7, operation_id='delete:queued')
            with self.assertRaises((RuntimeError, ValueError)):
                pending.result(2)
        self.assertEqual(self.control.call_count, 1)
        self.assertEqual([item.sandbox_id for item in self.registry.growth_intents()], ['one'])

    def test_imported_unknown_primary_terminal_releases_forecast(self):
        self.service.admit_managed_continuation('one', 7, 'imported-request')
        self.assertEqual(self.registry.growth_intents()[0].job_id, '')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)
        self.control.side_effect = lambda registration, payload, **kwargs: {
            **self.response(registration, payload),
            'job': {**self.response(registration, payload)['job'], 'state': 'exited'},
        }
        self.assertTrue(self.service.managed_process_status('one', 'primary').terminal)
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 0)
        self.assertEqual(self.registry.growth_intents()[0].phase, 'terminal')
        # Only a successful supervisor replay binds the host launch identity.
        self.assertTrue(self.service.start_managed_process('one', self.spec).terminal)
        self.assertEqual(self.registry.growth_intents()[0].job_id, 'primary')
        with self.assertRaises(DirectRegistryConflictError):
            self.service.start_managed_process('one', replace(self.spec, job_id='different'))

    def test_crossed_capture_requotes_full_growth_before_sparse_restore(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        # Real serialized sparse metadata: restore I/O is tiny while potential
        # primary growth remains 4 GiB. A fake allocated-bytes object misses
        # precisely the distinction this regression protects.
        store = self.service.warden.artifacts
        store.root.chmod(0o700)
        directory = store.prepare_generation(sandbox_id='one', sandbox_generation=7, hibernation_generation=1)
        files = []
        for name, role in (('application_memory.img', HibernationFileRole.MAIN_MEMORY),
                           ('checkpoint.img', HibernationFileRole.KERNEL_STATE),
                           ('pages_meta.img', HibernationFileRole.ALLOCATOR_METADATA)):
            path = directory / name
            with path.open('wb') as handle:
                handle.write(b'x' * 4096)
                if role == HibernationFileRole.MAIN_MEMORY:
                    handle.truncate(8 << 20)
            files.append(LocalHibernationArtifactFile.from_path(path, role=role))
        registration = self.registry.get('one')
        store.publish_complete(HibernationManifest(
            sandbox_id='one', sandbox_generation=7, hibernation_generation=1,
            operation_id='park:crossed', spec_sha256=registration.spec_sha256,
            container_id=registration.container_id, created_ns=1,
            runtime=self.service.warden.config.runtime_fingerprint, files=tuple(files),
            managed_process_sha256='', version=2))
        capture_started, finish_capture, forecast_retired, return_capture = (Event() for _ in range(4))
        native_park = self.service.warden.park
        observe_park = self.service._observe_managed_park

        def capture(*args, **kwargs):
            capture_started.set()
            self.assertTrue(finish_capture.wait(2))
            return native_park(*args, **kwargs)

        def retire(*args, **kwargs):
            observe_park(*args, **kwargs)
            forecast_retired.set()
            self.assertTrue(return_capture.wait(2))

        runtime = DirectNodeRuntime(self.service)
        with (ThreadPoolExecutor(max_workers=2) as pool,
              patch.object(self.service.warden, 'park', side_effect=capture),
              patch.object(self.service, '_observe_managed_park', side_effect=retire),
              patch.object(self.service.warden, 'resume', wraps=self.service.warden.resume) as resume):
            park = pool.submit(runtime.park, 'one', operation_id='park:crossed')
            self.assertTrue(capture_started.wait(1))
            wake = pool.submit(runtime.wake_with_activity_revision, 'one', generation=7,
                               operation_id='wake:crossed', relay_request_id='request-one')
            # The actual continuation grants growth before joining the old
            # capture's exclusive lifecycle operation.
            deadline = time.monotonic() + 1
            while not self.registry.relay_wake_fence('one', 7, 'request-one') and time.monotonic() < deadline:
                time.sleep(.005)
            self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))
            finish_capture.set()
            self.assertTrue(forecast_retired.wait(1))
            with self.service._reserve_active_capacity('peer', 1, ResourceQuantity(memory_mb=4096)):
                return_capture.set()
                self.assertEqual(park.result(1).state, 'parked')
                self.wait_for_demand()
                cost = self.service._restore_cost('one', 7, ResourceQuantity(memory_mb=4096))
                self.assertEqual(cost.memory_bytes, 4 << 30)
                self.assertLess(cost.read_bytes, 1 << 20)
                self.assertFalse(wake.done())
                resume.assert_not_called()
            self.assertEqual(wake.result(2)[0].state, 'running')
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 4 << 30)

    def test_continuation_timeout_is_retryable_without_revoking_live_wait(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        self.service.start_managed_process('two', self.spec)
        self.service.admission_wait_seconds = .25
        runtime = DirectNodeRuntime(self.service)
        with patch.object(runtime._warm_parks, 'wake', wraps=runtime._warm_parks.wake) as cancel_wait:
            with self.assertRaises(SandboxStartupBusyError):
                runtime.wake_with_activity_revision('one', generation=7,
                    operation_id='wake:wait', relay_request_id='request-one')
            cancel_wait.assert_not_called()
        self.assertFalse(self.registry.relay_wake_fence('one', 7, 'request-one'))
        self.assertEqual(self.service.get('one').state, 'running')
        self.service.observe_managed_wait('two', 7, 'request-two')
        self.service.admission_wait_seconds = 3
        record, _ = runtime.wake_with_activity_revision('one', generation=7,
            operation_id='wake:wait', relay_request_id='request-one')
        self.assertEqual(record.state, 'running')
        self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))

    def test_restore_failure_after_growth_grant_is_not_a_safe_admission_retry(self):
        self.service.start_managed_process('one', self.spec)
        self.service.observe_managed_wait('one', 7, 'request-one')
        self.service.park('one', operation_id='park:one')
        runtime = DirectNodeRuntime(self.service)
        with patch.object(self.service.warden, 'resume', side_effect=RuntimeError('ambiguous native restore')):
            with self.assertRaisesRegex(RuntimeError, 'ambiguous native restore') as caught:
                runtime.wake_with_activity_revision('one', generation=7,
                    operation_id='wake:failed', relay_request_id='request-one')
        self.assertNotIsInstance(caught.exception, SandboxStartupBusyError)
        self.assertTrue(self.registry.relay_wake_fence('one', 7, 'request-one'))
        self.assertEqual(self.registry.growth_intents()[0].phase, 'active')

    def test_restart_preserves_parked_managed_migration_authority(self):
        self.service.start_managed_process('one', self.spec)
        self.service.park('one', operation_id='park:migration')
        registration = self.registry.get('one')
        moving = self.registry.begin_move_out('one', expected_revision=registration.revision,
                                              migration_id='migration:owned', migration_sha256='e'*64)
        restarted = DirectSandboxService(self.provisioner)
        try:
            records = restarted.start()
            self.assertEqual(next(item.state for item in records if item.spec.id == 'one'), 'moving_out')
            self.assertEqual(self.registry.get('one'), moving)
            self.assertEqual(self.registry.growth_intents()[0].phase, 'parked')
            self.assertEqual(restarted.warm_park_demand().physical_bytes, 0)
        finally:
            restarted.stop()

    def test_previous_registry_v4_migrates_preserving_registration(self):
        # An exact former schema with no growth rows, not hand-written SQLite.
        path = Path(self.tmp.name) / 'legacy.sqlite'
        legacy = DirectSandboxRegistry(path)
        record = legacy.plan(spec=self.fixture.spec(), sandbox_generation=1,
                             operation_id='create:legacy', runtime_compatibility_sha256='b'*64)
        with closing(sqlite3.connect(path)) as conn:
            conn.execute('DROP TABLE managed_growth')
            conn.execute('DROP TABLE reflink_overlaps')
            conn.execute('DROP TABLE workspace_capacity')
            conn.execute('DROP TABLE registration_disk')
            conn.execute('PRAGMA user_version=4')
        reopened = DirectSandboxRegistry(path)
        self.assertEqual(reopened.get('sandbox'), record)
        self.assertEqual(reopened.growth_intents(), ())
