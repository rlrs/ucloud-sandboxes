import unittest
from threading import Event, Thread
from types import SimpleNamespace

from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.sandbox import (
    NodeDrainState,
    SandboxBusyError,
    SandboxSnapshotPublicationPendingError,
)


class _Registry:
    def __init__(self, registrations: tuple[object, ...]) -> None:
        self._registrations = registrations

    def load_drain(self) -> NodeDrainState:
        return NodeDrainState()

    def list(self) -> tuple[object, ...]:
        return self._registrations


class _IdleService:
    idle_park_seconds = 0.01

    def __init__(self, registrations: tuple[object, ...]) -> None:
        self.provisioner = SimpleNamespace(registry=_Registry(registrations))

    def open_admission(self) -> None:
        pass

    def close_admission(self) -> None:
        pass

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
            spec=SimpleNamespace(parkable=True, managed_process=True),
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

    def test_wake_publication_fence_is_owned_by_the_runtime(self) -> None:
        service = _WakeService()
        service.publication_pending = True
        manager = DirectNodeRuntime(service)  # type: ignore[arg-type]

        with self.assertRaises(SandboxSnapshotPublicationPendingError):
            manager.wake(
                "agent",
                generation=1,
                operation_id="relay-wake:request-3",
            )

        self.assertFalse(service.wake_calls)

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


if __name__ == "__main__":
    unittest.main()
