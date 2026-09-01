import unittest
from collections import deque
from io import StringIO
import sys
from threading import Condition, Event, Thread
from types import SimpleNamespace
from unittest.mock import patch

from ucloud_sandboxes.models import utc_now
from ucloud_sandboxes.sandbox_exec import (
    ExecSession,
    ExecSessionManager,
    SandboxExecSpec,
    new_exec_session_id,
)


class SandboxExecProtocolTests(unittest.TestCase):
    def test_exec_payload_requires_the_canonical_schema(self) -> None:
        payload = {
            "command": ["/bin/echo", "ok"],
            "env": {"MODE": "test"},
            "working_dir": "/workspace",
            "stdin": False,
            "tty": False,
        }

        spec = SandboxExecSpec.from_dict(payload, sandbox_id="sandbox-one")

        self.assertEqual(spec.command, ("/bin/echo", "ok"))
        self.assertEqual(spec.env, {"MODE": "test"})
        for invalid in (
            {**payload, "command": "/bin/echo"},
            {**payload, "env": {"MODE": 1}},
            {**payload, "stdin": 0},
            {**payload, "sandbox_id": "sandbox-one"},
        ):
            with self.assertRaises(ValueError):
                SandboxExecSpec.from_dict(invalid, sandbox_id="sandbox-one")

    def test_blocked_stdin_write_does_not_hold_the_manager_lock(self) -> None:
        manager = ExecSessionManager(FakeSandboxManager())
        pipe = BlockingStdin()
        session = _install_session(manager, pipe)
        failures: list[BaseException] = []
        write_done = Event()
        close_done = Event()

        def write() -> None:
            try:
                manager.write_stdin(session.id, "payload")
            except BaseException as exc:  # pragma: no cover - thread handoff
                failures.append(exc)
            finally:
                write_done.set()

        def close() -> None:
            try:
                manager.close_stdin(session.id)
            except BaseException as exc:  # pragma: no cover - thread handoff
                failures.append(exc)
            finally:
                close_done.set()

        writer = Thread(target=write)
        closer = Thread(target=close)
        writer.start()
        self.assertTrue(pipe.write_started.wait(1))
        closer.start()
        self.assertFalse(close_done.wait(0.05))

        probe_done = Event()
        probe = Thread(
            target=lambda: (manager.get(session.id), probe_done.set()),
        )
        probe.start()
        try:
            self.assertTrue(probe_done.wait(0.2))
        finally:
            pipe.release.set()
        writer.join(1)
        closer.join(1)
        probe.join(1)

        self.assertTrue(write_done.is_set())
        self.assertTrue(close_done.is_set())
        self.assertTrue(pipe.closed)
        self.assertFalse(failures)

    def test_popen_validation_failure_unwinds_fence_and_activity_lease(self) -> None:
        sandbox_manager = FakeSandboxManager()
        manager = ExecSessionManager(sandbox_manager)
        spec = SandboxExecSpec(
            sandbox_id="sandbox-one",
            command=("/bin/echo", "ok"),
            stdin=True,
        )

        with patch(
            "ucloud_sandboxes.sandbox_exec.subprocess.Popen",
            side_effect=ValueError("embedded null byte"),
        ):
            session = manager.start(spec)

        self.assertEqual(session.status, "failed")
        self.assertEqual(session.exit_code, 1)
        self.assertFalse(session.stdin_open)
        self.assertFalse(session.activity_lease)
        self.assertEqual(sandbox_manager.lifecycle.acquired, ["sandbox-one"])
        self.assertEqual(sandbox_manager.lifecycle.released, ["sandbox-one"])
        self.assertEqual(sandbox_manager.runtime.start_failed, ["sandbox-one"])
        self.assertEqual(sandbox_manager.runtime.started, [])
        self.assertEqual(sandbox_manager.capacity_acquired, ["sandbox-one"])
        self.assertEqual(sandbox_manager.capacity_released, ["capacity:sandbox-one"])
        self.assertTrue(
            any("embedded null byte" in event.data for event in session.events)
        )

    def test_output_thread_start_failure_kills_process_and_unwinds_state(self) -> None:
        sandbox_manager = FakeSandboxManager()
        manager = ExecSessionManager(sandbox_manager)
        process = FakeProcess()
        original_start = Thread.start
        starts = 0

        def fail_second_start(thread: Thread) -> None:
            nonlocal starts
            starts += 1
            if starts == 2:
                raise RuntimeError("cannot start stderr pump")
            original_start(thread)

        with (
            patch(
                "ucloud_sandboxes.sandbox_exec.subprocess.Popen",
                return_value=process,
            ),
            patch(
                "ucloud_sandboxes.sandbox_exec.Thread.start",
                new=fail_second_start,
            ),
        ):
            session = manager.start(
                SandboxExecSpec(
                    sandbox_id="sandbox-one",
                    command=("/bin/echo", "ok"),
                    stdin=True,
                )
            )

        self.assertEqual(session.status, "failed")
        self.assertFalse(session.activity_lease)
        self.assertEqual(process.kill_calls, 1)
        self.assertEqual(process.wait_calls, 1)
        self.assertTrue(process.stdin.closed)
        self.assertTrue(process.stdout.closed)
        self.assertTrue(process.stderr.closed)
        self.assertEqual(sandbox_manager.runtime.started, ["sandbox-one"])
        self.assertEqual(sandbox_manager.runtime.start_failed, ["sandbox-one"])
        self.assertEqual(sandbox_manager.lifecycle.released, ["sandbox-one"])
        self.assertEqual(sandbox_manager.capacity_acquired, ["sandbox-one"])
        self.assertEqual(sandbox_manager.capacity_released, ["capacity:sandbox-one"])
        self.assertTrue(
            any("cannot start stderr pump" in event.data for event in session.events)
        )

    def test_flushed_short_output_is_visible_before_process_exit(self) -> None:
        manager = ExecSessionManager(FakeSandboxManager())
        session = manager.start(
            SandboxExecSpec(
                sandbox_id="sandbox-one",
                command=(
                    sys.executable,
                    "-c",
                    (
                        "import sys; "
                        "sys.stdout.write('early\\n'); sys.stdout.flush(); "
                        "sys.stdin.read(1); "
                        "sys.stdout.write('late\\n'); sys.stdout.flush()"
                    ),
                ),
                stdin=True,
            )
        )

        live_events = manager.events_after(
            session.id,
            after=1,
            wait_seconds=1.0,
        )

        self.assertEqual(
            "".join(event.data for event in live_events if event.stream == "stdout"),
            "early\n",
        )
        self.assertEqual(session.status, "running")
        manager.write_stdin(session.id, "x")
        after = max(event.sequence for event in live_events)
        while session.status == "running":
            terminal_events = manager.events_after(
                session.id,
                after=after,
                wait_seconds=1.0,
            )
            if terminal_events:
                after = terminal_events[-1].sequence
        self.assertEqual(session.exit_code, 0)

    def test_output_decoder_preserves_split_utf8_code_points(self) -> None:
        manager = ExecSessionManager(FakeSandboxManager())
        session = _install_session(manager, BlockingStdin())
        pipe = ChunkedBinaryPipe((b"price: \xe2", b"\x82", b"\xac", b""))

        manager._pump_stream(session.id, "stdout", pipe)  # noqa: SLF001

        self.assertTrue(pipe.closed)
        self.assertEqual(
            "".join(
                event.data for event in session.events if event.stream == "stdout"
            ),
            "price: €",
        )

    def test_exec_spec_rejects_nul_before_acquiring_runtime_state(self) -> None:
        for spec in (
            SandboxExecSpec("sandbox-one", ("bad\0command",)),
            SandboxExecSpec("sandbox-one", ("ok",), env={"A": "bad\0value"}),
            SandboxExecSpec("sandbox-one", ("ok",), working_dir="/bad\0path"),
        ):
            with self.assertRaisesRegex(ValueError, "NUL"):
                spec.validate()

    def test_exec_signal_targets_the_running_process_and_is_idempotent_terminal(
        self,
    ) -> None:
        manager = ExecSessionManager(FakeSandboxManager())
        process = FakeProcess()
        session = _install_session(manager, process.stdin)
        session.process = process

        signaled = manager.signal(session.id, 15)
        session.status = "failed"
        session.process = None
        terminal = manager.signal(session.id, 9)

        self.assertIs(signaled, session)
        self.assertIs(terminal, session)
        self.assertEqual(process.signals, [15])
        for invalid in (0, 65, True):
            with self.assertRaisesRegex(ValueError, "signal"):
                manager.signal(session.id, invalid)


class FakeLifecycle:
    def __init__(self) -> None:
        self.acquired: list[str] = []
        self.released: list[str] = []

    def acquire_shared(self, sandbox_id: str) -> None:
        self.acquired.append(sandbox_id)

    def release_shared(self, sandbox_id: str) -> None:
        self.released.append(sandbox_id)


class FakeRuntime:
    def __init__(self) -> None:
        self.started: list[str] = []
        self.start_failed: list[str] = []

    def exec_command(self, _sandbox_id, command, **_kwargs):  # type: ignore[no-untyped-def]
        return tuple(command)

    def exec_started(self, sandbox_id: str) -> None:
        self.started.append(sandbox_id)

    def exec_start_failed(self, sandbox_id: str) -> None:
        self.start_failed.append(sandbox_id)


class FakeSandboxManager:
    def __init__(self) -> None:
        self.lifecycle = FakeLifecycle()
        self.runtime = FakeRuntime()
        self.capacity_acquired: list[str] = []
        self.capacity_released: list[str] = []

    def require_activity_sandbox(self, _sandbox_id: str) -> object:
        return object()

    def acquire_exec_capacity(self, sandbox_id: str) -> str:
        self.capacity_acquired.append(sandbox_id)
        return f"capacity:{sandbox_id}"

    def release_exec_capacity(self, token: str) -> None:
        self.capacity_released.append(token)


class BlockingStdin:
    def __init__(self) -> None:
        self.write_started = Event()
        self.release = Event()
        self.closed = False

    def write(self, _data: str) -> None:
        self.write_started.set()
        self.release.wait(1)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class FakeProcess:
    def __init__(self) -> None:
        self.stdin = StringIO()
        self.stdout = StringIO()
        self.stderr = StringIO()
        self.kill_calls = 0
        self.wait_calls = 0
        self.signals: list[int] = []

    def kill(self) -> None:
        self.kill_calls += 1

    def wait(self) -> int:
        self.wait_calls += 1
        return 1

    def send_signal(self, signal: int) -> None:
        self.signals.append(signal)


class ChunkedBinaryPipe:
    encoding = "utf-8"
    errors = "replace"

    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        self.buffer = self
        self._chunks = iter(chunks)
        self.closed = False

    def read1(self, _size: int) -> bytes:
        return next(self._chunks)

    def close(self) -> None:
        self.closed = True


def _install_session(
    manager: ExecSessionManager,
    pipe: BlockingStdin,
) -> ExecSession:
    spec = SandboxExecSpec(
        sandbox_id="sandbox-one",
        command=("/bin/cat",),
        stdin=True,
    )
    now = utc_now()
    session = ExecSession(
        id=new_exec_session_id(),
        spec=spec,
        argv=spec.command,
        status="running",
        created_at=now,
        updated_at=now,
        condition=Condition(manager._lock),  # noqa: SLF001
        stdin_open=True,
        events=deque(maxlen=32),
        process=SimpleNamespace(stdin=pipe),
    )
    with manager._lock:  # noqa: SLF001
        manager._sessions[session.id] = session  # noqa: SLF001
    return session


if __name__ == "__main__":
    unittest.main()
