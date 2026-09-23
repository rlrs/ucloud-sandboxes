import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from ucloud_sandboxes.memory_filesystem import (
    _provision_locked, MemoryFilesystemError, provision_ram_filesystem,
)


class RamFilesystemTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve() / "active"
        self.size = 1024**3 * 95 // 100 // 4096 * 4096
        self.info = {"target": "/", "source": "/dev/root", "fstype": "ext4", "options": "rw"}
        self.commands = []
        self.addCleanup(patch.stopall)
        patch("ucloud_sandboxes.memory_filesystem.os.geteuid", return_value=0).start()
        patch("ucloud_sandboxes.memory_filesystem._private").start()
        patch("ucloud_sandboxes.memory_filesystem.Path.read_text", return_value="MemTotal: 1048576 kB\n").start()
        patch("ucloud_sandboxes.memory_filesystem._mount", side_effect=lambda _: self.info).start()
        patch("ucloud_sandboxes.memory_filesystem.os.statvfs", return_value=SimpleNamespace(f_blocks=self.size // 4096, f_frsize=4096)).start()
        patch("ucloud_sandboxes.memory_filesystem._run", side_effect=self.mount).start()

    def mount(self, *command):
        self.commands.append(command)
        self.info = {"target": str(self.root), "source": "ucloud-application-memory", "fstype": "tmpfs", "options": "rw,noswap,nodev,nosuid"}
        return ""

    def test_capacity_clamps_to_guest_and_live_mount_is_reused(self):
        first = provision_ram_filesystem(self.root, capacity_bytes=2 * 1024**3)
        (self.root / "live").write_bytes(b"owned memory")
        self.assertEqual(provision_ram_filesystem(self.root, capacity_bytes=2 * 1024**3), first)
        self.assertEqual(first["capacity_bytes"], self.size)
        self.assertEqual(len(self.commands), 1)
        self.assertIn("noswap", self.commands[0][4])
        self.assertEqual((self.root / "live").read_bytes(), b"owned memory")

    def test_unmounted_files_are_never_covered(self):
        self.root.mkdir()
        (self.root / "orphan").touch()
        with self.assertRaisesRegex(MemoryFilesystemError, "cover existing"):
            provision_ram_filesystem(self.root, capacity_bytes=1024**3)
        self.assertFalse(self.commands)

    def test_wrong_mount_or_swap_enabled_is_rejected_without_remount(self):
        self.root.mkdir()
        for field, value in (("source", "another-owner"), ("fstype", "ext4"), ("options", "rw")):
            self.mount()
            self.commands.clear()
            self.info[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(MemoryFilesystemError, "capacity contract"):
                provision_ram_filesystem(self.root, capacity_bytes=1024**3)
            self.assertFalse(self.commands)

    def test_existing_mount_capacity_is_never_resized(self):
        self.root.mkdir()
        self.mount()
        self.commands.clear()
        with self.assertRaisesRegex(MemoryFilesystemError, "capacity contract"):
            provision_ram_filesystem(self.root, capacity_bytes=512 * 1024**2)
        self.assertFalse(self.commands)


class MemoryFilesystemTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name)
        self.mount = self.parent / "mounts"
        self.mount.mkdir(mode=0o700)
        self.commands = []
        self.mounted = False
        self.uuid = ""
        self.direct = True
        self.fail_format = False
        self.partial_format = False
        self.addCleanup(patch.stopall)
        patch(
            "ucloud_sandboxes.memory_filesystem._mount",
            side_effect=lambda _path: self.mount_info(),
        ).start()
        patch(
            "ucloud_sandboxes.memory_filesystem._run", side_effect=self.run_command
        ).start()
        patch("ucloud_sandboxes.memory_filesystem.XfsMemoryQuota.validate_root").start()

    def mount_info(self):
        return {
            "target": str(self.mount) if self.mounted else "/",
            "source": "/dev/loop99" if self.mounted else "/dev/root",
            "fstype": "xfs" if self.mounted else "ext4",
            "options": "rw,prjquota" if self.mounted else "rw",
        }

    def run_command(self, *command):
        self.commands.append(command)
        if command[0] == "findmnt":
            return "/"
        if command[0] == "mkfs.xfs":
            self.uuid = command[2].split("uuid=")[1]
            if self.partial_format:
                with (self.parent / "memory-backing.xfs").open("r+b") as stream:
                    stream.write(b"partially formatted")
                    stream.flush()
            if self.fail_format:
                raise OSError("injected mkfs failure")
            return ""
        if command[0] == "blkid":
            return self.uuid if "UUID" in command else "xfs"
        if command[0] == "losetup":
            if "--json" in command:
                return json.dumps(
                    {
                        "loopdevices": [
                            {
                                "name": "/dev/loop99",
                                "back-file": str(self.parent / "memory-backing.xfs"),
                                "dio": self.direct,
                            }
                        ]
                    }
                )
            return "/dev/loop99"
        if command[0] == "mount":
            self.mounted = True
            return ""
        raise AssertionError(command)

    def test_fresh_image_record_and_reboot_never_reformat(self):
        result = _provision_locked(self.mount, 1024**2)
        self.assertEqual(result["phase"], "formatted")
        self.assertEqual(sum(c[0] == "mkfs.xfs" for c in self.commands), 1)
        self.mounted = False  # Reboot: image/receipt survive, loop mount does not.
        self.assertEqual(_provision_locked(self.mount, 1024**2), result)
        self.assertEqual(sum(c[0] == "mkfs.xfs" for c in self.commands), 1)

    def test_legacy_worker_and_unrecorded_image_are_never_formatted(self):
        (self.mount / "old-sandbox").mkdir()
        with self.assertRaisesRegex(MemoryFilesystemError, "fresh-worker"):
            _provision_locked(self.mount, 1024**2)
        (self.mount / "old-sandbox").rmdir()
        (self.parent / "memory-backing.xfs").touch()
        with self.assertRaisesRegex(MemoryFilesystemError, "unrecorded"):
            _provision_locked(self.mount, 1024**2)
        self.assertFalse(any(c[0] == "mkfs.xfs" for c in self.commands))

    def test_partial_format_is_ambiguous_and_requires_recovery(self):
        self.fail_format = self.partial_format = True
        with self.assertRaises(OSError):
            _provision_locked(self.mount, 1024**2)
        self.fail_format = False
        with self.assertRaisesRegex(MemoryFilesystemError, "interrupted"):
            _provision_locked(self.mount, 1024**2)
        self.assertEqual(sum(c[0] == "mkfs.xfs" for c in self.commands), 1)

    def test_changed_inode_or_capacity_and_buffered_loop_fail_closed(self):
        _provision_locked(self.mount, 1024**2)
        with self.assertRaisesRegex(MemoryFilesystemError, "configuration differs"):
            _provision_locked(self.mount, 2 * 1024**2)
        self.direct = False
        with self.assertRaisesRegex(MemoryFilesystemError, "direct loop"):
            _provision_locked(self.mount, 1024**2)
        image = self.parent / "memory-backing.xfs"
        image.rename(self.parent / "original")
        with image.open("wb") as stream:
            stream.truncate((self.parent / "original").stat().st_size)
        image.chmod(0o600)
        with self.assertRaisesRegex(MemoryFilesystemError, "identity changed"):
            _provision_locked(self.mount, 1024**2)
