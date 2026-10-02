import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.test_images import _uploaded_context
from ucloud_sandboxes.build_deadline import (
    ImageBuildTimeoutError,
    build_execution_deadline,
    remaining_build_execution_seconds,
    without_build_execution_deadline,
)
from ucloud_sandboxes.images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageBuildStore,
    ImageManager,
    ImageStore,
)
from ucloud_sandboxes.sandbox import CommandResult

TEST_TIER = "contract"


class BuildDeadlineTests(unittest.TestCase):
    def test_nested_stages_share_budget_and_finalizers_restore_it(self):
        clock = [100.0]
        with patch("ucloud_sandboxes.build_deadline.time.monotonic", side_effect=lambda: clock[0]):
            self.assertIsNone(remaining_build_execution_seconds())
            self.assertEqual(remaining_build_execution_seconds(60), 60)
            with build_execution_deadline(10):
                clock[0] += 3
                with build_execution_deadline(60):
                    self.assertEqual(remaining_build_execution_seconds(), 7)
                    self.assertEqual(remaining_build_execution_seconds(2), 2)
                clock[0] += 7
                with self.assertRaises(ImageBuildTimeoutError):
                    remaining_build_execution_seconds()
                with without_build_execution_deadline():
                    self.assertEqual(remaining_build_execution_seconds(5), 5)
                with self.assertRaises(ImageBuildTimeoutError):
                    remaining_build_execution_seconds()
            self.assertIsNone(remaining_build_execution_seconds())

    def test_invalid_server_budget_is_rejected(self):
        for value in (0, -1, float("nan"), float("inf"), True, None, "10"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                with build_execution_deadline(value):
                    self.fail("invalid execution budget accepted")


@unittest.skipUnless(os.name == "posix", "owned process groups require POSIX")
class BuildProcessDeadlineTests(unittest.TestCase):
    def _manager(self, root, runtime, *, timeout=0.3, publisher=None):
        return ImageManager(
            ImageStore(root / "images.sqlite"), runtime,
            max_active_builds=1, max_queued_builds=0,
            build_execution_timeout_seconds=timeout,
            environment_publisher=publisher,
        )

    def _submit(self, manager, name, *, push=False, cleanup=None):
        identity, materialize = _uploaded_context(("Dockerfile", b"FROM scratch\n"))
        return manager.start_build(
            ImageBuildSpec(id=name, tag=f"fixture/{name}:latest", context_path="."),
            context_identity=identity, materialize_context=materialize,
            push=push, cleanup=cleanup,
        )[0]

    def test_silent_ignoring_child_times_out_persists_and_releases_slot(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            pid_path = root / "child.pid"

            class Runtime(DockerImageRuntime):
                def build_command(self, spec, **kwargs):
                    source = "print('next build completed')" if spec.id == "next" else (
                        "import os,signal,time; from pathlib import Path; "
                        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                        f"Path({str(pid_path)!r}).write_text(str(os.getpid())); time.sleep(60)"
                    )
                    return (sys.executable, "-c", source)

            manager = self._manager(root, Runtime())
            cleaned = []

            def cleanup():
                # Finalizers remain allowed to execute after the work deadline.
                cleaned.append(remaining_build_execution_seconds(5))

            record = self._submit(manager, "hang", cleanup=cleanup)
            early = manager.wait_for_build(record.build_id, timeout_seconds=0.01)
            self.assertEqual(early.status, "running")  # Client wait is independent.
            started = time.monotonic()
            done = manager.wait_for_build(record.build_id, timeout_seconds=5)
            self.assertLess(time.monotonic() - started, 3)
            self.assertEqual(done.status, "failed")
            self.assertIn("server execution deadline", done.error)
            self.assertEqual(cleaned, [5])
            self.assertEqual(manager.active_build_count(), 0)
            self.assertEqual(ImageBuildStore(root / "images.sqlite").get(record.build_id), done)
            self.assertIsNone(manager.get_image("hang"))
            self.assertFalse(Path(record.context_path).exists())
            self.assertTrue(pid_path.exists(), "owned child never started")
            with self.assertRaises(ChildProcessError):
                os.waitpid(int(pid_path.read_text()), os.WNOHANG)

            next_record = self._submit(manager, "next")
            self.assertEqual(manager.wait_for_build(next_record.build_id, timeout_seconds=5).status, "succeeded")
            self.assertEqual(manager.active_build_count(), 0)

    def test_stdout_eof_does_not_bypass_deadline_without_output_callback(self):
        real_popen = subprocess.Popen
        owned = []

        def spawn(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            owned.append(process)
            return process

        with patch("ucloud_sandboxes.images.subprocess.Popen", side_effect=spawn):
            with self.assertRaises(ImageBuildTimeoutError), build_execution_deadline(0.2):
                DockerImageRuntime()._run((sys.executable, "-c",
                    "import os,time; os.close(1); os.close(2); time.sleep(60)"))
        self.assertEqual(len(owned), 1)
        self.assertIsNotNone(owned[0].returncode)
        self.assertTrue(owned[0].stdout.closed)

    def test_inherited_stdout_child_is_stopped_without_touching_unrelated_process(self):
        with TemporaryDirectory() as raw:
            pid_path = Path(raw) / "descendant.pid"
            child_code = (
                "import os,signal,time; from pathlib import Path; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"Path({str(pid_path)!r}).write_text(str(os.getpid())); time.sleep(60)"
            )
            leader_code = (
                "import subprocess,sys,time; from pathlib import Path; "
                f"subprocess.Popen([sys.executable, '-c', {child_code!r}]); "
                f"p=Path({str(pid_path)!r});\n"
                "while not p.exists(): time.sleep(.001)\n"
                # Leader exits; the descendant still keeps the output FD open.
            )
            unrelated = subprocess.Popen((sys.executable, "-c", "import time; time.sleep(60)"),
                                         start_new_session=True)
            try:
                with self.assertRaises(ImageBuildTimeoutError), build_execution_deadline(0.4):
                    DockerImageRuntime()._run_streaming((sys.executable, "-c", leader_code),
                                                       on_output=lambda *_: None)
                self.assertIsNone(unrelated.poll())
                self.assertTrue(pid_path.exists())
                pid = int(pid_path.read_text())
                # An adopted zombie may await init's reaper in a container, but
                # must not retain a runnable process or the inherited stdout FD.
                until = time.monotonic() + 2
                while time.monotonic() < until:
                    try:
                        state = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[0]
                    except FileNotFoundError:
                        break
                    if state == "Z":
                        break
                    time.sleep(0.01)
                else:
                    self.fail("owned descendant still running after timeout")
            finally:
                unrelated.terminate()
                unrelated.wait(timeout=5)

    def test_one_deadline_covers_build_and_push(self):
        with TemporaryDirectory() as raw:
            class Runtime(DockerImageRuntime):
                def build_command(self, spec, **kwargs):
                    return (sys.executable, "-c", "import time; time.sleep(.16)")

                def push_command(self, image):
                    return (sys.executable, "-c", "import time; time.sleep(.4)")

            manager = self._manager(Path(raw), Runtime(), timeout=0.4)
            record = self._submit(manager, "shared-budget", push=True)
            done = manager.wait_for_build(record.build_id, timeout_seconds=5)
            self.assertEqual(done.status, "failed")
            self.assertEqual(done.exit_code, 0)  # Build completed; push did not.
            self.assertIn("server execution deadline", done.error)
            self.assertIn("docker_push_ms", done.timings["phases"])
            self.assertIsNone(manager.get_image("shared-budget"))

    def test_publication_uses_same_budget_and_cannot_publish_late_success(self):
        # Stages spend the budget only through this clock, never host load.
        now = [1000.0]
        clock = SimpleNamespace(monotonic=lambda: now[0])
        with TemporaryDirectory() as raw, patch(
            "ucloud_sandboxes.build_deadline.time", clock
        ):
            class Runtime(DockerImageRuntime):
                def build(self, spec, **kwargs):
                    now[0] += 0.06
                    return CommandResult(argv=("fixture-build",), exit_code=0)

            seen = []

            def publish(spec):
                seen.append(remaining_build_execution_seconds())
                # A legacy callback that returns after expiry must not record a
                # successful image, even though arbitrary Python is not preempted.
                now[0] += 0.1
                return "sha256:" + "a" * 64

            manager = self._manager(Path(raw), Runtime(buildx_direct_push=True),
                                    timeout=0.12, publisher=publish)
            record = self._submit(manager, "late-publish", push=True)
            done = manager.wait_for_build(record.build_id, timeout_seconds=5)
            self.assertEqual(done.status, "failed")
            self.assertIn("server execution deadline", done.error)
            self.assertEqual(len(seen), 1)
            self.assertAlmostEqual(seen[0], 0.06)
            self.assertIsNone(manager.get_image("late-publish"))
            self.assertEqual(manager.active_build_count(), 0)

    def test_partial_utf8_and_callback_error_leave_no_reader_or_child(self):
        delivered = []
        with build_execution_deadline(2):
            result = DockerImageRuntime()._run_streaming((sys.executable, "-c",
                "import os,time; os.write(1,b'\\xe2'); time.sleep(.02); "
                "os.write(1,b'\\x82\\xac\\xff\\r'); time.sleep(.02); os.write(1,b'\\n')"),
                on_output=lambda _, chunk: delivered.append(chunk))
        self.assertEqual(result.stdout, "\u20ac\ufffd\n")
        self.assertEqual("".join(delivered), result.stdout)

        owned = []
        real_popen = subprocess.Popen

        def spawn(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            owned.append(process)
            return process

        def callback(*_):
            raise ValueError("fixture callback failed")

        with patch("ucloud_sandboxes.images.subprocess.Popen", side_effect=spawn):
            with self.assertRaisesRegex(ValueError, "fixture callback failed"), build_execution_deadline(2):
                DockerImageRuntime()._run_streaming((sys.executable, "-c",
                    "import time; print('ready',flush=True); time.sleep(60)"), on_output=callback)
        self.assertIsNotNone(owned[0].returncode)
        self.assertTrue(owned[0].stdout.closed)


if __name__ == "__main__":
    unittest.main()
