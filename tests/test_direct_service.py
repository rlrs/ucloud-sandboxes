import sys
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from types import SimpleNamespace
from unittest.mock import patch

from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.direct_service import DirectProcessRunner, DirectSandboxService
from ucloud_sandboxes.memory_backing import MemoryBackingBusyError, MemoryBackingStore
from ucloud_sandboxes.sandbox import SandboxDeleteBusyError, SandboxFileTooLargeError
from tests.test_split_memory_lifecycle import FakeQuota

TEST_TIER = "contract"


class DirectProcessRunnerTests(unittest.TestCase):
    def test_file_stdin_is_consumed_without_materializing_it_in_python(self) -> None:
        with TemporaryFile() as source:
            source.write(b"binary\0payload" * 100000)
            source.seek(0)
            result = DirectProcessRunner().run(
                (sys.executable, "-c", "import sys; print(len(sys.stdin.buffer.read()))"),
                input_bytes=None, input_file=source, timeout_seconds=5,
                max_stdout_bytes=1024, max_stderr_bytes=1024,
            )
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, b"1400000\n")
            self.assertFalse(source.closed)

    def test_streams_stdin_and_captures_binary_output(self) -> None:
        result = DirectProcessRunner().run(
            (
                sys.executable,
                "-c",
                "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()[::-1])",
            ),
            input_bytes=b"\0abc",
            timeout_seconds=5,
            max_stdout_bytes=1024,
            max_stderr_bytes=1024,
        )
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, b"cba\0")

    def test_terminates_host_exec_when_output_bound_is_exceeded(self) -> None:
        with self.assertRaisesRegex(SandboxFileTooLargeError, "bounded output"):
            DirectProcessRunner().run(
                (
                    sys.executable,
                    "-c",
                    "import sys; sys.stdout.buffer.write(b'x' * 1048576)",
                ),
                input_bytes=None,
                timeout_seconds=5,
                max_stdout_bytes=1024,
                max_stderr_bytes=1024,
            )


class _Registry:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.rows = {}

    def get(self, sandbox_id):
        return self.rows.get(sandbox_id)

    def growth_intents(self):
        return ()


class _Provisioner:
    """Commit phase deleting, then drop split memory like the real owner."""

    def __init__(self, root: Path, reference: MemoryBackingRef) -> None:
        self.registry = _Registry(root / "registry.sqlite")
        self.warden = SimpleNamespace(config=SimpleNamespace())
        self.store = MemoryBackingStore(
            root / "memory", root / "state.sqlite",
            hard_capacity_bytes=100, quota=FakeQuota(),
        )
        self.reference = reference
        self.store.prepare(reference, sandbox_id="s", sandbox_generation=1)
        self.registry.rows["s"] = SimpleNamespace(
            sandbox_id="s", sandbox_generation=1, phase="owned")
        self.attempts = 0

    def delete(self, sandbox_id, *, generation=None):
        self.attempts += 1
        registration = self.registry.get(sandbox_id)
        if registration is None:
            return
        registration.phase = "deleting"
        self.store.delete(self.reference, sandbox_id=sandbox_id,
                          sandbox_generation=generation)
        self.registry.rows.pop(sandbox_id)


class DirectDeleteDrainTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.reference = MemoryBackingRef("s.sandbox-1", 80)
        self.provisioner = _Provisioner(Path(directory.name), self.reference)
        self.service = DirectSandboxService(self.provisioner)

    def test_delete_waits_unlocked_for_publication_reader_to_abort(self) -> None:
        store, registry = self.provisioner.store, self.provisioner.registry
        leased = threading.Event()

        def publish() -> None:
            # Mirrors the split publisher: a lease across chunked upload, with
            # an ownership check under the lifecycle lock at every chunk.
            with store.read_lease(self.reference, sandbox_id="s", sandbox_generation=1):
                leased.set()
                while True:
                    with self.service._lock("s", 1):
                        if registry.get("s").phase != "owned":
                            return
                    time.sleep(0.01)

        publisher = threading.Thread(target=publish, daemon=True)
        self.service._publication_threads[("s", 1)] = publisher
        publisher.start()
        self.assertTrue(leased.wait(5))

        self.service.delete("s", generation=1)

        self.assertFalse(publisher.is_alive())
        self.assertIsNone(registry.get("s"))
        self.assertGreaterEqual(self.provisioner.attempts, 2)
        self.assertEqual(store.metrics()["memory_backing_hard_reserved_bytes"], 0)

    def test_delete_deadline_raises_retryable_error_and_retry_completes(self) -> None:
        store, registry = self.provisioner.store, self.provisioner.registry
        with store.read_lease(self.reference, sandbox_id="s", sandbox_generation=1):
            with patch("ucloud_sandboxes.direct_service._DELETE_DRAIN_DEADLINE_SECONDS", 0.3):
                with self.assertRaises(SandboxDeleteBusyError) as raised:
                    self.service.delete("s", generation=1)
        self.assertIsInstance(raised.exception.__cause__, MemoryBackingBusyError)
        self.assertEqual(registry.get("s").phase, "deleting")

        self.service.delete("s", generation=1)
        self.assertIsNone(registry.get("s"))

    def test_generation_removed_while_waiting_is_already_deleted(self) -> None:
        registry = self.provisioner.registry

        def busy_then_reconciled(sandbox_id, *, generation=None):
            self.provisioner.attempts += 1
            # The periodic reconciler completes the delete while we wait.
            registry.rows.pop(sandbox_id)
            raise MemoryBackingBusyError("memory allocation still has publication readers")

        self.provisioner.delete = busy_then_reconciled
        self.service.delete("s", generation=1)
        self.assertEqual(self.provisioner.attempts, 1)


if __name__ == "__main__":
    unittest.main()
