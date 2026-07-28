import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.hibernation_quota_helper import (
    QuotaHelperConfig,
    QuotaHelperError,
    XfsHibernationQuotaHelper,
    load_config,
    render_hibernation_quota_helper_script,
    run_action,
)


class FakeXfs:
    def __init__(self, mount_root: Path) -> None:
        self.mount_root = mount_root
        self.projects: dict[str, int] = {}
        self.project_inherit: set[str] = set()
        self.limits_mb: dict[int, int] = {}
        self.valid_mount = True
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command) -> str:
        command = tuple(command)
        self.commands.append(command)
        name = Path(command[0]).name
        if name == "findmnt":
            if not self.valid_mount:
                return "/ ext4 rw,relatime\n"
            return f"{self.mount_root} xfs rw,relatime,prjquota\n"
        if name == "xfs_io":
            operation = command[command.index("-c") + 1]
            path = command[-1]
            if operation.startswith("chproj -R "):
                self.projects[path] = int(operation.removeprefix("chproj -R "))
                return ""
            if operation == "chattr +P":
                self.project_inherit.add(path)
                return ""
            if operation == "stat -v":
                flags = (
                    "fsxattr.xflags = 0x200 [proj-inherit]\n"
                    if path in self.project_inherit
                    else "fsxattr.xflags = 0x0 []\n"
                )
                return flags + f"fsxattr.projid = {self.projects.get(path, 0)}\n"
        if name == "xfs_quota":
            operation = command[command.index("-c") + 1]
            if operation.startswith("limit -p "):
                match = re.fullmatch(
                    r"limit -p bsoft=([0-9]+)(m?) bhard=([0-9]+)(m?) ([0-9]+)",
                    operation,
                )
                assert match is not None
                assert match.group(1) == match.group(3)
                assert match.group(2) == match.group(4)
                self.limits_mb[int(match.group(5))] = int(match.group(3))
                return ""
            if operation == "report -p -b -N -n":
                return "".join(
                    f"#{project} 0 {limit * 1024} {limit * 1024} 00 [--------]\n"
                    for project, limit in sorted(self.limits_mb.items())
                    if limit
                )
        raise AssertionError(f"unexpected command: {command!r}")


class HibernationQuotaHelperTests(unittest.TestCase):
    def _helper(self, root: Path) -> tuple[XfsHibernationQuotaHelper, FakeXfs]:
        root = root.resolve()
        mount_root = root / "mount"
        quota_root = mount_root / "ucloud-hibernation"
        quota_root.mkdir(parents=True, mode=0o700)
        executables = {}
        for name in ("xfs_io", "xfs_quota", "findmnt"):
            path = root / name
            path.write_text("#!/bin/sh\n", encoding="ascii")
            path.chmod(0o700)
            executables[name] = path
        config = QuotaHelperConfig(
            mount_root=mount_root,
            quota_root=quota_root,
            xfs_io=executables["xfs_io"],
            xfs_quota=executables["xfs_quota"],
            findmnt=executables["findmnt"],
        )
        fake = FakeXfs(mount_root)
        return (
            XfsHibernationQuotaHelper(
                config,
                runner=fake,
                require_root_ownership=False,
            ),
            fake,
        )

    def test_prepare_inspect_list_and_drop_are_exact(self) -> None:
        with TemporaryDirectory() as raw_dir:
            helper, fake = self._helper(Path(raw_dir))
            ready = helper.prepare("sandbox-1", 7, 200_001, 3136)
            self.assertEqual(
                ready,
                {
                    "hard_limit_mb": 3136,
                    "path": str(helper.config.quota_root / "sandbox-1.sandbox-7"),
                    "project_id": 200_001,
                    "sandbox_generation": 7,
                    "sandbox_id": "sandbox-1",
                    "state": "ready",
                },
            )
            self.assertEqual(helper.inspect("sandbox-1", 7), ready)
            self.assertEqual(helper.prepare("sandbox-1", 7, 200_001, 3136), ready)
            self.assertEqual(helper.list_state()["reservations"], [ready])

            dropped = helper.drop("sandbox-1", 7, 200_001)
            self.assertTrue(dropped["removed"])
            self.assertEqual(fake.limits_mb[200_001], 0)
            self.assertFalse(Path(ready["path"]).exists())
            replayed = helper.drop("sandbox-1", 7, 200_001)
            self.assertFalse(replayed["removed"])

    def test_prepare_rejects_existing_directory_from_another_project(self) -> None:
        with TemporaryDirectory() as raw_dir:
            helper, fake = self._helper(Path(raw_dir))
            path = helper.config.quota_root / "sandbox-1.sandbox-7"
            path.mkdir(mode=0o700)
            fake.projects[str(path)] = 999
            with self.assertRaisesRegex(QuotaHelperError, "another XFS project"):
                helper.prepare("sandbox-1", 7, 200_001, 3136)
            self.assertNotIn(200_001, fake.limits_mb)

    def test_list_accepts_private_artifact_store_lock_in_shared_root(self) -> None:
        with TemporaryDirectory() as raw_dir:
            helper, _fake = self._helper(Path(raw_dir))
            store_lock = helper.config.quota_root / ".store.lock"
            store_lock.write_bytes(b"")
            store_lock.chmod(0o600)

            self.assertEqual(helper.list_state()["reservations"], [])

            store_lock.chmod(0o644)
            with self.assertRaisesRegex(QuotaHelperError, "private owned"):
                helper.list_state()

    def test_helper_fails_closed_without_xfs_project_quota_mount(self) -> None:
        with TemporaryDirectory() as raw_dir:
            helper, fake = self._helper(Path(raw_dir))
            fake.valid_mount = False
            with self.assertRaisesRegex(QuotaHelperError, "XFS project-quota mount"):
                helper.prepare("sandbox-1", 7, 200_001, 3136)
            self.assertEqual(
                list(helper.config.quota_root.iterdir()),
                [helper.config.quota_root / ".quota.lock"],
            )

    def test_action_and_argument_validation_are_fail_closed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            helper, _fake = self._helper(Path(raw_dir))
            with self.assertRaises(QuotaHelperError):
                run_action(helper, ["prepare", "../escape", "7", "1", "1"])
            with self.assertRaisesRegex(QuotaHelperError, "usage"):
                run_action(helper, ["prepare", "sandbox-1"])
            with self.assertRaisesRegex(QuotaHelperError, "project id"):
                helper.prepare("sandbox-1", 7, 0, 3136)

    def test_config_is_strict_and_rendered_helper_is_pinned(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            helper, _fake = self._helper(root)
            config_path = root / "quota-helper.json"
            config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "mount_root": str(helper.config.mount_root),
                        "quota_root": str(helper.config.quota_root),
                        "xfs_io": str(helper.config.xfs_io),
                        "xfs_quota": str(helper.config.xfs_quota),
                        "findmnt": str(helper.config.findmnt),
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                load_config(config_path, require_root_ownership=False),
                helper.config,
            )
            raw = json.loads(config_path.read_text(encoding="utf-8"))
            raw["unexpected"] = True
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(QuotaHelperError, "invalid schema"):
                load_config(config_path, require_root_ownership=False)
            with self.assertRaisesRegex(QuotaHelperError, "cannot be opened safely"):
                load_config(root / "missing.json", require_root_ownership=False)

            rendered = render_hibernation_quota_helper_script(
                config_path="/fixed/hibernation-quota.json"
            )
            self.assertTrue(rendered.startswith("#!/usr/bin/python3\n"))
            self.assertIn(
                "DEFAULT_CONFIG_PATH = '/fixed/hibernation-quota.json'",
                rendered,
            )


if __name__ == "__main__":
    unittest.main()
