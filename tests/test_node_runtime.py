import unittest
from contextlib import nullcontext
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import Mock

from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.sandbox import (
    NodeDrainState,
    SandboxBusyError,
    SandboxExecAdmissionDeferredError,
)


class _Registry:
    def __init__(self, registrations: tuple[object, ...]) -> None:
        self._registrations = registrations

    def load_drain(self) -> NodeDrainState:
        return NodeDrainState()

    def list(self) -> tuple[object, ...]:
        return self._registrations

    def get(self, sandbox_id):
        return next((r for r in self._registrations if r.sandbox_id == sandbox_id), None)

    def activity_revision(self) -> int:
        return 1

    def snapshot(self) -> object:
        return SimpleNamespace(records=self._registrations, activity_revision=1)


class _IdleService:
    idle_park_seconds = 0.01

    def __init__(self, registrations: tuple[object, ...]) -> None:
        self.provisioner = SimpleNamespace(registry=_Registry(registrations))

    def open_admission(self) -> None:
        pass

    def close_admission(self) -> None:
        pass

    def resident_demand_snapshot(self):
        return {"admitted_demand_bytes": 0, "pending_demand_bytes": 0,
                "unknown_transition_memory_costs": 0}

    def resident_memory_ram_bytes(self, sandbox_id, generation, sample):
        return None if sample is None else sample.shared_memory_bytes

    def idle_for_seconds(self, *_args: object, **_kwargs: object) -> float:
        return 1.0

    def get(self, sandbox_id: str) -> object:
        return SimpleNamespace(spec=SimpleNamespace(id=sandbox_id), state="running")


class _WakeService(_IdleService):
    idle_park_seconds = 0

    def __init__(self) -> None:
        registration = SimpleNamespace(
            phase="owned",
            sandbox_id="agent",
            sandbox_generation=1,
            spec=SimpleNamespace(parkable=True, managed_process=True, memory_mb=256),
        )
        super().__init__((registration,))
        self.wake_calls: list[tuple[str, int, str]] = []
        self.park_calls: list[str] = []
        self.publication_pending = False
        self.activity_revision = 100

    def storage_native_publication_pending(self, _sandbox_id: str) -> bool:
        return self.publication_pending

    def wake(self, sandbox_id: str, *, generation: int, operation_id: str) -> object:
        self.wake_calls.append((sandbox_id, generation, operation_id))
        return SimpleNamespace(state="running")

    def park(self, sandbox_id: str, **_kwargs: object) -> object:
        self.park_calls.append(sandbox_id)
        return SimpleNamespace(state="parked")

    def advance_lifecycle_activity_revision(self) -> int:
        self.activity_revision += 1
        return self.activity_revision


class DirectNodeRuntimeTests(unittest.TestCase):
    def test_tool_joins_transition_before_reading_registration_or_restoring(self) -> None:
        for deleted in (False, True):
            with self.subTest(deleted=deleted):
                service = _WakeService()
                registration = SimpleNamespace(
                    sandbox_generation=2, to_direct_sandbox=lambda: "new-generation",
                )
                service._require_registration = Mock(return_value=registration)
                service._request_lock = Mock(side_effect=lambda *_: nullcontext())
                service.mark_activity = Mock()
                service.ensure_running_with_timings = Mock(return_value={})
                manager = DirectNodeRuntime(service)
                entered, finished = Event(), Event()
                failures = []

                def tool():
                    entered.set()
                    try:
                        with manager.lifecycle.shared("agent"):
                            pass
                    except Exception as exc:
                        failures.append(exc)
                    finally:
                        finished.set()

                with manager.lifecycle._coordinator.exclusive("agent"):
                    thread = Thread(target=tool)
                    thread.start()
                    self.assertTrue(entered.wait(1))
                    self.assertFalse(finished.wait(.05))
                    service._require_registration.assert_not_called()
                    service.ensure_running_with_timings.assert_not_called()
                    if deleted:
                        service._require_registration.side_effect = ValueError("sandbox deleted")
                thread.join(1)
                self.assertTrue(finished.is_set())
                if deleted:
                    self.assertEqual([str(exc) for exc in failures], ["sandbox deleted"])
                    service.ensure_running_with_timings.assert_not_called()
                else:
                    self.assertFalse(failures)
                    service._request_lock.assert_called_once_with("agent", 2)
                    service.ensure_running_with_timings.assert_called_once_with("new-generation")
                with manager.lifecycle._coordinator.exclusive("agent"):
                    pass  # Neither branch leaks an activity lease.

    def test_tool_transition_timeout_is_retryable_before_exec_acceptance(self) -> None:
        service = _WakeService()
        service.admission_wait_seconds = .01
        service._require_registration = Mock()
        manager = DirectNodeRuntime(service)
        with manager.lifecycle._coordinator.exclusive("agent"):
            with self.assertRaises(SandboxExecAdmissionDeferredError):
                manager.lifecycle.acquire_shared("agent")
        service._require_registration.assert_not_called()
        with manager.lifecycle._coordinator.exclusive("agent"):
            pass

    def test_activity_release_unwinds_coordinator_when_storage_cleanup_fails(self) -> None:
        for failing_step in ("registration", "mark_activity"):
            with self.subTest(failing_step=failing_step):
                service = _WakeService()
                service.provisioner.registry.get = Mock(
                    return_value=SimpleNamespace(sandbox_generation=1),
                    side_effect=RuntimeError("registry unavailable") if failing_step == "registration" else None,
                )
                service.mark_activity = Mock(
                    side_effect=RuntimeError("activity update failed") if failing_step == "mark_activity" else None,
                )
                manager = DirectNodeRuntime(service)  # type: ignore[arg-type]
                coordinator = manager.lifecycle._coordinator
                coordinator.acquire_shared("agent")
                with self.assertRaises(RuntimeError):
                    manager.lifecycle.release_shared("agent")
                # The error remains visible while park/delete can proceed.
                with coordinator.exclusive("agent"):
                    pass
                if failing_step == "registration":
                    service.mark_activity.assert_not_called()
                else:
                    service.mark_activity.assert_called_once_with("agent", 1)

    def test_wake_is_idempotent_while_running_activity_is_attached(self) -> None:
        service = _WakeService()
        manager = DirectNodeRuntime(service)  # type: ignore[arg-type]
        manager.lifecycle._coordinator.acquire_shared("agent")
        try:
            record = manager.wake(
                "agent",
                generation=1,
                operation_id="relay-wake:request-1",
            )
        finally:
            manager.lifecycle._coordinator.release_shared("agent")

        self.assertEqual(record.state, "running")
        self.assertEqual(
            service.wake_calls,
            [("agent", 1, "relay-wake:request-1")],
        )
        self.assertEqual(service.activity_revision, 101)

    def test_wake_joins_an_existing_transition_then_rechecks_state(self) -> None:
        service = _WakeService()
        manager = DirectNodeRuntime(service)  # type: ignore[arg-type]
        started = Event()
        finished = Event()
        failures: list[BaseException] = []

        def wake() -> None:
            started.set()
            try:
                manager.wake(
                    "agent",
                    generation=1,
                    operation_id="relay-wake:request-2",
                )
            except BaseException as exc:  # pragma: no cover - thread handoff
                failures.append(exc)
            finally:
                finished.set()

        with manager.lifecycle._coordinator.exclusive("agent"):
            thread = Thread(target=wake)
            thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(finished.wait(0.05))
        thread.join(1)

        self.assertTrue(finished.is_set())
        self.assertFalse(failures)
        self.assertEqual(len(service.wake_calls), 1)

    def test_park_still_rejects_attached_activity(self) -> None:
        service = _WakeService()
        manager = DirectNodeRuntime(service)  # type: ignore[arg-type]
        manager.lifecycle._coordinator.acquire_shared("agent")
        try:
            with self.assertRaisesRegex(SandboxBusyError, "start_agent"):
                manager.park(
                    "agent",
                    operation_id="relay-park:request-1",
                )
        finally:
            manager.lifecycle._coordinator.release_shared("agent")

        self.assertFalse(service.park_calls)

    def test_local_wake_reaches_storage_while_publication_is_pending(self) -> None:
        service = _WakeService()
        service.publication_pending = True
        manager = DirectNodeRuntime(service)  # type: ignore[arg-type]

        record, revision = manager.wake_with_activity_revision(
            "agent",
            generation=1,
            operation_id="relay-wake:request-3",
        )
        self.assertEqual(record.state, "running")
        self.assertEqual(revision, 101)
        self.assertEqual(service.wake_calls, [("agent", 1, "relay-wake:request-3")])

    def test_idle_parking_uses_lifecycle_and_skips_managed_agents(self) -> None:
        registrations = (
            SimpleNamespace(
                phase="owned",
                sandbox_id="interactive",
                sandbox_generation=1,
                spec=SimpleNamespace(parkable=True, managed_process=False),
            ),
            SimpleNamespace(
                phase="owned",
                sandbox_id="agent",
                sandbox_generation=1,
                spec=SimpleNamespace(parkable=True, managed_process=True),
            ),
        )
        manager = DirectNodeRuntime(_IdleService(registrations))  # type: ignore[arg-type]
        parked = Event()
        calls: list[tuple[str, bool]] = []

        def park(sandbox_id: str, *, operation_id: str, background: bool) -> None:
            self.assertTrue(operation_id.startswith("idle-park:"))
            calls.append((sandbox_id, background))
            parked.set()

        manager.park = park  # type: ignore[method-assign]
        manager.start()
        try:
            self.assertTrue(parked.wait(timeout=1))
        finally:
            manager.stop()

        self.assertTrue(calls)
        self.assertEqual(
            {sandbox_id for sandbox_id, _background in calls}, {"interactive"}
        )
        self.assertTrue(all(background for _sandbox_id, background in calls))

    def test_idle_parker_reuses_inventory_until_external_registry_revision_changes(self) -> None:
        managed = SimpleNamespace(phase='owned', sandbox_id='managed', sandbox_generation=1,
                                  spec=SimpleNamespace(parkable=True, managed_process=True))
        interactive = SimpleNamespace(phase='owned', sandbox_id='interactive', sandbox_generation=1,
                                      spec=SimpleNamespace(parkable=True, managed_process=False))
        service = _IdleService((managed,))
        registry = service.provisioner.registry
        registry.activity_revision = Mock(side_effect=[1, 1, 2, 2])
        registry.snapshot = Mock(side_effect=[
            SimpleNamespace(records=(managed,), activity_revision=1),
            SimpleNamespace(records=(managed, interactive), activity_revision=2),
        ])
        manager = DirectNodeRuntime(service)  # type: ignore[arg-type]
        manager._background_stop = Mock()
        manager._background_stop.wait.side_effect = [False, False, False, False, True]
        manager.park = Mock()
        manager._idle_parking_loop()
        self.assertEqual(registry.snapshot.call_count, 2)
        self.assertEqual([call.args[0] for call in manager.park.call_args_list], ['interactive', 'interactive'])


if __name__ == "__main__":
    unittest.main()


class RelayLifecycleFenceTests(unittest.TestCase):
    def test_wake_intent_prevents_late_park_even_when_restore_failed(self):
        from unittest.mock import Mock
        from ucloud_sandboxes.sandbox import SandboxConflictError
        seen=set()
        registry=Mock()
        def fence(sandbox,generation,request,*,record=False):
            if record:
                seen.add((sandbox,generation,request))
            return (sandbox,generation,request) in seen
        registry.relay_wake_fence.side_effect=fence
        service=Mock()
        service.provisioner.registry=registry
        service.wake.side_effect=RuntimeError('restore temporarily blocked')
        manager=DirectNodeRuntime(service)
        with self.assertRaisesRegex(RuntimeError,'temporarily blocked'):
            manager.wake_with_activity_revision('s1',generation=1,operation_id='wake:r',relay_request_id='r')
        with self.assertRaisesRegex(SandboxConflictError,'superseded'):
            manager.park_with_activity_revision('s1',generation=1,operation_id='park:r',relay_request_id='r')
        service.park.assert_not_called()


class WarmRelayParkTests(unittest.TestCase):
    def test_reply_during_grace_avoids_checkpoint_and_durable_fence_survives_new_manager(self):
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkPolicy
        from ucloud_sandboxes.sandbox import SandboxConflictError
        from ucloud_sandboxes.warm_park import WarmParkDeferred
        seen = set()
        service = _WakeService()
        def fence(sandbox, generation, request, *, record=False):
            key = (sandbox, generation, request)
            if record:
                seen.add(key)
            return key in seen
        service.provisioner.registry.relay_wake_fence = fence
        manager = DirectNodeRuntime(service)
        manager._warm_parks = WarmParkPolicy(lambda: Pressure(.8, 0))
        key = ('agent', 1, 'request')
        with self.assertRaises(WarmParkDeferred):
            manager.park_with_activity_revision('agent', generation=1,
                operation_id='park:req', relay_request_id='request')
        self.assertFalse(manager._warm_parks._pending)
        self.assertIn(key, manager._warm_parks._waiting_since)
        record, _ = manager.wake_with_activity_revision('agent', generation=1,
                      operation_id='wake:req', relay_request_id='request')
        self.assertEqual(record.state, 'running')
        with self.assertRaisesRegex(SandboxConflictError, 'superseded'):
            manager.park_with_activity_revision('agent', generation=1,
                operation_id='park:req', relay_request_id='request')
        self.assertFalse(service.park_calls)
        restarted = DirectNodeRuntime(service)
        with self.assertRaisesRegex(SandboxConflictError, 'durable wake'):
            restarted.park_with_activity_revision('agent', generation=1,
                operation_id='park:replay', relay_request_id='request')
        self.assertFalse(service.park_calls)


class LocalRelayParkTests(unittest.TestCase):
    def make_runtime(self):
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkPolicy
        service = _WakeService()
        seen = set()
        def fence(sandbox, generation, request, *, record=False):
            from ucloud_sandboxes.direct_registry import DirectRegistryConflictError
            if generation != service.provisioner.registry._registrations[0].sandbox_generation:
                raise DirectRegistryConflictError('generation changed')
            key = (sandbox, generation, request)
            if record:
                seen.add(key)
            return key in seen
        service.provisioner.registry.relay_wake_fence = fence
        manager = DirectNodeRuntime(service)
        self.addCleanup(manager.stop)
        pressure = [Pressure(.8, 0)]
        manager._warm_parks = WarmParkPolicy(lambda: pressure[0])
        # Drive individual ticks deterministically; production start() runs
        # this same check even when the independent idle parker is disabled.
        manager._relay_parking_thread = SimpleNamespace(is_alive=lambda: True, join=lambda **_: None)
        return manager, service, pressure

    def recheck(self, manager):
        manager._recheck_relay_parks()
        for task in tuple(manager._relay_park_tasks.values()):
            task.result(timeout=2)

    def defer(self, manager):
        from ucloud_sandboxes.warm_park import WarmParkDeferred
        with self.assertRaises(WarmParkDeferred) as raised:
            manager.park_with_activity_revision('agent', generation=1,
                operation_id='park:req', relay_request_id='request')
        self.assertEqual(raised.exception.seconds, 30)
        self.assertEqual(len(manager._deferred_relay_parks), 1)

    def test_pressure_parks_without_another_http_request_or_polling_registry(self):
        from ucloud_sandboxes.background_io import Pressure
        manager, service, pressure = self.make_runtime()
        self.defer(manager)
        original = service.provisioner.registry.relay_wake_fence
        service.provisioner.registry.relay_wake_fence = Mock(side_effect=original)
        for _ in range(10):
            self.recheck(manager)
        service.provisioner.registry.relay_wake_fence.assert_not_called()
        self.assertFalse(service.park_calls)
        pressure[0] = Pressure(.01, 20)
        self.recheck(manager)
        self.assertEqual(service.park_calls, ['agent'])
        self.assertFalse(manager._deferred_relay_parks)

    def test_wake_cancels_local_park_and_replay(self):
        from ucloud_sandboxes.background_io import Pressure
        manager, service, pressure = self.make_runtime()
        self.defer(manager)
        manager.wake_with_activity_revision('agent', generation=1,
            operation_id='wake:req', relay_request_id='request')
        pressure[0] = Pressure(.01, 20)
        self.recheck(manager)
        self.assertFalse(manager._deferred_relay_parks)
        self.assertFalse(service.park_calls)

    def test_replaced_generation_cannot_be_parked(self):
        from ucloud_sandboxes.background_io import Pressure
        manager, service, pressure = self.make_runtime()
        self.defer(manager)
        service.provisioner.registry._registrations[0].sandbox_generation = 2
        pressure[0] = Pressure(.01, 20)
        self.recheck(manager)
        self.assertFalse(manager._deferred_relay_parks)
        self.assertFalse(service.park_calls)

    def test_failed_checkpoint_keeps_intent(self):
        from ucloud_sandboxes.background_io import Pressure
        manager, service, pressure = self.make_runtime()
        self.defer(manager)
        pressure[0] = Pressure(.01, 20)
        service.park = Mock(side_effect=RuntimeError('temporary I/O failure'))
        self.recheck(manager)
        self.assertEqual(len(manager._deferred_relay_parks), 1)
        self.recheck(manager)
        service.park.assert_called_once()
        entry = next(iter(manager._deferred_relay_parks.values()))
        entry['retry_at'] = 0
        manager._warm_parks._retry_after.clear()
        manager._warm_parks._settle_until = 0
        service.park.side_effect = None
        service.park.return_value = SimpleNamespace(state='parked')
        self.recheck(manager)
        self.assertFalse(manager._deferred_relay_parks)

    def test_restarted_worker_accepts_durable_retry(self):
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkPolicy
        manager, service, pressure = self.make_runtime()
        self.defer(manager)
        restarted = DirectNodeRuntime(service)
        restarted._warm_parks = WarmParkPolicy(lambda: Pressure(.01, 20))
        restarted.park_with_activity_revision('agent', generation=1,
            operation_id='park:req', relay_request_id='request')
        self.assertEqual(service.park_calls, ['agent'])

    def test_parker_runs_when_idle_parking_is_disabled(self):
        manager = DirectNodeRuntime(_WakeService())
        tick = Event()
        manager._recheck_relay_parks = tick.set
        manager.start()
        thread = manager._relay_parking_thread
        try:
            self.assertTrue(tick.wait(2))
            self.assertIsNone(manager._idle_parking_thread)
        finally:
            manager.stop()
        self.assertFalse(thread.is_alive())

    def test_slow_checkpoint_does_not_block_another_selected_safe_wait(self):
        from copy import copy
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkDeferred
        from ucloud_sandboxes.resident_memory import ResidentMemorySample
        manager, service, pressure = self.make_runtime()
        sample = ResidentMemorySample(
            current_bytes=1024**3, anonymous_bytes=1024**3, file_bytes=0,
            dirty_bytes=0, writeback_bytes=0, refault_file_pages=0,
            cgroup_path='/owned', cgroup_device=1, cgroup_inode=1,
            sentry_pid=1, sentry_start_time_ticks=1, sampled_at=0,
        )
        from dataclasses import replace
        import time
        service.resident_memory_sample = lambda *_: replace(sample, sampled_at=time.monotonic())
        manager._relay_park_workers = 2
        registry = service.provisioner.registry
        other = copy(registry._registrations[0])
        other.sandbox_id = 'other'
        registry._registrations += (other,)
        self.defer(manager)
        with self.assertRaises(WarmParkDeferred):
            manager.park_with_activity_revision('other', generation=1,
                operation_id='park:other', relay_request_id='other-request')
        slow_started, release_slow, other_done = Event(), Event(), Event()
        original = service.park
        def park(sandbox_id, **kwargs):
            if sandbox_id == 'agent':
                slow_started.set()
                if not release_slow.wait(2):
                    raise RuntimeError('test checkpoint timed out')
            else:
                other_done.set()
                if not release_slow.wait(2):
                    raise RuntimeError('test checkpoint timed out')
            return original(sandbox_id, **kwargs)
        service.park = park
        pressure[0] = Pressure(.01, 20, 0, 1024**3)
        try:
            manager._recheck_relay_parks()
            self.assertTrue(slow_started.wait(1))
            self.assertTrue(other_done.wait(1))
            # Repeated ticks do not queue another task for the blocked park.
            for _ in range(10):
                manager._recheck_relay_parks()
            self.assertLessEqual(len(manager._relay_park_tasks), 2)
        finally:
            release_slow.set()
            for task in tuple(manager._relay_park_tasks.values()):
                task.result(timeout=2)
        self.assertEqual(sorted(service.park_calls), ['agent', 'other'])
        self.assertEqual(manager.resident_wait_snapshot()['checkpoints_completed'], 2)

    def test_missing_footprints_do_not_fan_out_foreground_or_background_captures(self):
        from copy import copy
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkDeferred

        manager, service, pressure = self.make_runtime()
        manager._relay_park_workers = 32
        registry = service.provisioner.registry
        names = ['agent'] + [f'other-{index}' for index in range(16)]
        for name in names[1:]:
            record = copy(registry._registrations[0])
            record.sandbox_id = name
            registry._registrations += (record,)
        for name in names:
            with self.assertRaises(WarmParkDeferred):
                manager.park_with_activity_revision(
                    name, generation=1, operation_id='park:request',
                    relay_request_id='request',
                )
        started, release = Event(), Event()
        original = service.park
        calls = []

        def park(sandbox_id, **kwargs):
            calls.append(sandbox_id)
            started.set()
            if not release.wait(2):
                raise RuntimeError('test checkpoint timed out')
            return original(sandbox_id, **kwargs)

        service.park = park
        pressure[0] = Pressure(.01, 20, 50, 1024**3)
        try:
            manager._recheck_relay_parks()
            self.assertTrue(started.wait(1))
            for name in names[1:]:
                with self.assertRaises(WarmParkDeferred):
                    manager.park_with_activity_revision(
                        name, generation=1, operation_id='park:request',
                        relay_request_id='request',
                    )
            manager._recheck_relay_parks()
            self.assertEqual(calls, ['agent'])
            self.assertEqual(manager.resident_wait_snapshot()['checkpoint_inflight'], 1)
        finally:
            release.set()
            for task in tuple(manager._relay_park_tasks.values()):
                task.result(timeout=2)

    def test_stopped_local_recheck_keeps_durable_intent_without_parking(self):
        from ucloud_sandboxes.background_io import Pressure
        manager, service, pressure = self.make_runtime()
        self.defer(manager)
        pressure[0] = Pressure(.01, 20)
        manager.stop()
        manager._recheck_relay_parks()
        self.assertFalse(service.park_calls)
        self.assertEqual(len(manager._deferred_relay_parks), 1)
        self.assertIsNone(manager._relay_park_executor)
