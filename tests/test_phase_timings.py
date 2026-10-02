import threading
import unittest
from unittest import mock

from ucloud_sandboxes import phase_timings


class PhaseTimingsTests(unittest.TestCase):
    def test_phases_outside_a_recording_are_ignored(self) -> None:
        with phase_timings.phase("runsc_create"):
            pass
        with phase_timings.recording() as phases:
            pass
        self.assertEqual(phases, {})

    def test_repeated_phases_accumulate_and_nest(self) -> None:
        clock = iter([0.0, 0.010, 0.015, 0.040, 0.050, 0.070])
        with mock.patch.object(phase_timings.time, "monotonic", lambda: next(clock)):
            with phase_timings.recording() as phases:
                with phase_timings.phase("registry_commit"):
                    pass
                with phase_timings.phase("rootfs_prepare"):
                    with phase_timings.phase("registry_commit"):
                        pass
        self.assertEqual(phases, {"registry_commit_ms": 20, "rootfs_prepare_ms": 55})

    def test_failed_phase_is_still_recorded(self) -> None:
        with phase_timings.recording() as phases:
            with self.assertRaises(RuntimeError):
                with phase_timings.phase("runsc_start"):
                    raise RuntimeError("boom")
        self.assertIn("runsc_start_ms", phases)

    def test_recordings_are_scoped_to_their_thread(self) -> None:
        seen: list[dict[str, int]] = []

        def other_thread() -> None:
            with phase_timings.recording() as phases:
                with phase_timings.phase("network_ensure"):
                    pass
            seen.append(phases)

        with phase_timings.recording() as phases:
            thread = threading.Thread(target=other_thread)
            thread.start()
            thread.join()
        self.assertEqual(phases, {})
        self.assertEqual(list(seen[0]), ["network_ensure_ms"])


    def test_node_create_reports_phases_recorded_below_the_service(self) -> None:
        from types import SimpleNamespace

        from ucloud_sandboxes.node_runtime import DirectNodeRuntime

        def create(spec, *, operation):
            with phase_timings.phase("runsc_create"):
                pass
            with phase_timings.phase("registry_commit"):
                pass
            return "record"

        runtime = object.__new__(DirectNodeRuntime)
        runtime.service = SimpleNamespace(get_snapshot=lambda _id: None, create=create)
        record, timings = runtime.create_with_timings(
            SimpleNamespace(id="one"), operation=object()
        )
        self.assertEqual(record, "record")
        self.assertFalse(timings["idempotent"])
        self.assertEqual(
            sorted(timings["phases"]), ["registry_commit_ms", "runsc_create_ms"]
        )
        # The recording closes with the request; later phases are not captured.
        with phase_timings.phase("runsc_start"):
            pass
        self.assertNotIn("runsc_start_ms", timings["phases"])


if __name__ == "__main__":
    unittest.main()
