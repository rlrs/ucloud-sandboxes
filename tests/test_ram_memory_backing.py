from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.test_split_memory_lifecycle import FakeQuota
from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.memory_backing import MemoryBackingError, MemoryBackingStore


class RamMemoryBackingTests(unittest.TestCase):
    def test_live_ram_is_separate_and_recovers_empty_after_reboot(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            active = root / "ram"
            active.mkdir(mode=0o700)
            with patch(
                "ucloud_sandboxes.memory_backing.subprocess.run",
                return_value=SimpleNamespace(stdout="tmpfs rw,noswap\n"),
            ):
                store = MemoryBackingStore(
                    root / "disk",
                    root / "claims.sqlite",
                    hard_capacity_bytes=4096,
                    quota=FakeQuota(),
                    active_root=active,
                )
            ref = MemoryBackingRef("sandbox.sandbox-1", 4096)
            lease = store.prepare(ref, sandbox_id="sandbox", sandbox_generation=1)
            ram = active / ref.allocation_id
            self.assertNotEqual(lease.path, ram)
            (lease.path / "complete-checkpoint").write_text("durable")
            ram.rmdir()  # tmpfs contents disappear on reboot.
            store.require(ref, sandbox_id="sandbox", sandbox_generation=1)
            self.assertTrue(ram.is_dir())
            self.assertEqual(
                (lease.path / "complete-checkpoint").read_text(), "durable"
            )
            (ram / "application_memory.active").write_text("live")
            with store.read_lease(ref, sandbox_id="sandbox", sandbox_generation=1):
                with self.assertRaises(MemoryBackingError):
                    store.delete(ref, sandbox_id="sandbox", sandbox_generation=1)
                self.assertTrue(ram.exists())
                self.assertEqual(
                    store.metrics()["memory_backing_hard_reserved_bytes"], 4096
                )
            store.delete(ref, sandbox_id="sandbox", sandbox_generation=1)
            self.assertFalse(ram.exists())
            self.assertFalse(lease.path.exists())

    def test_swappable_tmpfs_cannot_silently_be_used_as_ram(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            active = root / "ram"
            active.mkdir(mode=0o700)
            with patch(
                "ucloud_sandboxes.memory_backing.subprocess.run",
                return_value=SimpleNamespace(stdout="tmpfs rw\n"),
            ):
                with self.assertRaisesRegex(MemoryBackingError, "requires tmpfs"):
                    MemoryBackingStore(
                        root / "disk",
                        root / "claims.sqlite",
                        hard_capacity_bytes=4096,
                        quota=FakeQuota(),
                        active_root=active,
                    )
