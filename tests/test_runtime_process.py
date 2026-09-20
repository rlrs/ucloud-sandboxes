"""Runtime provenance checks use synthetic procfs; no host PID is signalled."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.runtime_process import (
    RuntimeProcessIdentityError,
    owned_runtime_process_ticks,
)
from tests.test_direct_warden import write_process


class RuntimeProcessIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "runsc"
        self.binary.write_bytes(b"trusted executable")
        self.proc = self.root / "proc"
        write_process(self.proc, 100, 900)
        self.process = self.proc / "100"
        self.argv = [
            "runsc-sandbox",
            "--root=/runtime",
            "boot",
            "--bundle=/bundle",
            "owned",
        ]
        self.command_line()
        (self.process / "exe").symlink_to(self.binary)
        (self.process / "cgroup").write_text("0::/owned\n")

    def command_line(self):
        (self.process / "cmdline").write_bytes(("\0".join(self.argv) + "\0").encode())

    def verify(self, **kwargs):
        return owned_runtime_process_ticks(
            100,
            proc_root=self.proc,
            runsc=self.binary,
            runtime_root=Path("/runtime"),
            bundle=Path("/bundle"),
            container_id="owned",
            **kwargs,
        )

    def test_distribution_sentry_sidecar_is_accepted(self):
        sidecar = self.root / "gvisor-bin/gvisor_sentry"
        sidecar.parent.mkdir()
        sidecar.write_bytes(b"trusted sentry")
        (self.process / "exe").unlink()
        (self.process / "exe").symlink_to(sidecar)
        self.assertEqual(self.verify(expected_ticks=900), 900)

    def test_wrong_executable_root_bundle_or_container_is_rejected(self):
        for index, replacement in (
            (0, "python"),
            (1, "--root=/other"),
            (2, "gofer"),
            (3, "--bundle=/other"),
            (4, "other"),
        ):
            with self.subTest(replacement=replacement):
                original = self.argv[index]
                self.argv[index] = replacement
                self.command_line()
                with self.assertRaises(RuntimeProcessIdentityError):
                    self.verify()
                self.argv[index] = original
        self.command_line()
        other = self.root / "unrelated"
        other.write_bytes(b"other executable")
        (self.process / "exe").unlink()
        (self.process / "exe").symlink_to(other)
        with self.assertRaises(RuntimeProcessIdentityError):
            self.verify()

    def test_duplicate_flag_and_changed_start_time_are_rejected(self):
        self.argv.insert(2, "--root=/runtime")
        self.command_line()
        with self.assertRaises(RuntimeProcessIdentityError):
            self.verify()
        self.argv.pop(2)
        self.command_line()
        with self.assertRaises(RuntimeProcessIdentityError):
            self.verify(expected_ticks=899)

    def test_gofer_split_flags_are_accepted_only_for_gofer(self):
        self.argv = [
            "runsc-gofer",
            "--root",
            "/runtime",
            "gofer",
            "--bundle",
            "/bundle",
            "owned",
        ]
        self.command_line()
        self.assertEqual(self.verify(role="gofer"), 900)
        with self.assertRaises(RuntimeProcessIdentityError):
            self.verify()
