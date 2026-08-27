import unittest
from threading import Event
from types import SimpleNamespace

from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.sandbox import NodeDrainState


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


class DirectNodeRuntimeTests(unittest.TestCase):
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
