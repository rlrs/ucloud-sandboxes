import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from ucloud_sandboxes.gvisor_distribution import (
    GVISOR_COMMIT,
    GVISOR_SIDECARS,
    distribution_files,
    installed_sidecar_fingerprints,
    require_reflink_restore_runtime,
    REFLINK_RESTORE_PATCH_SHA256,
)


class GvisorDistributionTests(unittest.TestCase):
    def test_reflink_gate_requires_pinned_patch_and_complete_distribution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runsc = self.fixture(root)
            path = root / "build-manifest.json"
            manifest = json.loads(path.read_text())
            manifest["patches"] = [
                {"name": "20260817/0004-ucloud-abort-hibernation.patch",
                 "sha256": "c97ae915c984f50a554c9b79ede4b73a5ab68e0ad5d89469e8f09819468519fe"},
                {"name": "20260817/0006-ucloud-ram-application-memory.patch",
                 "sha256": "af09c7f45e666c4e7e46c5a999cc8414034deb069c7c25c9fdcf29c9b808310e"},
            ]
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "qualified reflink"):
                require_reflink_restore_runtime(runsc, GVISOR_COMMIT)
            entry = {"name": "20260817/0007-ucloud-reflink-application-memory.patch", "sha256": "0" * 64}
            manifest["patches"].append(entry)
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "qualified reflink"):
                require_reflink_restore_runtime(runsc, GVISOR_COMMIT)
            entry["sha256"] = REFLINK_RESTORE_PATCH_SHA256
            path.write_text(json.dumps(manifest))
            require_reflink_restore_runtime(runsc, GVISOR_COMMIT)
            (root / "gvisor-bin" / "gvisor_sentry").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "mismatch"):
                require_reflink_restore_runtime(runsc, GVISOR_COMMIT)

    def fixture(self, root):
        files = {}
        for name in ["runsc", *(f"gvisor-bin/{n}" for n in GVISOR_SIDECARS)]:
            path = root / name
            path.parent.mkdir(exist_ok=True)
            data = name.encode()
            path.write_bytes(data)
            path.chmod(0o755)
            files[name] = {
                "size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        manifest = {"schema": 2, "gvisor_commit": GVISOR_COMMIT, "files": files}
        (root / "build-manifest.json").write_text(json.dumps(manifest))
        return root / "runsc"

    def test_complete_distribution_and_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runsc = self.fixture(root)
            self.assertEqual(len(distribution_files(runsc, GVISOR_COMMIT)), 4)
            (root / "gvisor-bin/gvisor_sentry").write_bytes(b"wrong")
            with self.assertRaisesRegex(ValueError, "mismatch"):
                distribution_files(runsc, GVISOR_COMMIT)

    def test_rejects_missing_extra_symlink_and_nonexecutable_companions(self):
        for kind in ("missing", "extra", "symlink", "nonexecutable"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                runsc = self.fixture(root)
                sentry = root / "gvisor-bin/gvisor_sentry"
                if kind == "missing":
                    sentry.unlink()
                elif kind == "extra":
                    (sentry.parent / "unexpected").write_bytes(b"x")
                elif kind == "symlink":
                    content = sentry.read_bytes()
                    sentry.unlink()
                    (root / "elsewhere").write_bytes(content)
                    sentry.symlink_to(root / "elsewhere")
                else:
                    sentry.chmod(0o644)
                with self.assertRaises(ValueError):
                    distribution_files(runsc, GVISOR_COMMIT)

    def test_new_pin_requires_manifest_legacy_does_not(self):
        with tempfile.TemporaryDirectory() as directory:
            runsc = Path(directory) / "runsc"
            runsc.write_bytes(b"legacy")
            self.assertEqual(distribution_files(runsc, "9" * 40), [])
            with self.assertRaises(FileNotFoundError):
                distribution_files(runsc, GVISOR_COMMIT)

    def test_companion_change_changes_installed_checkpoint_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runsc = self.fixture(root)
            before = installed_sidecar_fingerprints(runsc, GVISOR_COMMIT)
            original_runsc = runsc.read_bytes()
            (root / "gvisor-bin/gvisor_sentry").write_bytes(b"different sentry")
            after = installed_sidecar_fingerprints(runsc, GVISOR_COMMIT)
            self.assertEqual(runsc.read_bytes(), original_runsc)
            self.assertNotEqual(before, after)
            with self.assertRaisesRegex(ValueError, "legacy"):
                installed_sidecar_fingerprints(runsc, "9" * 40)

    def test_service_fences_same_runsc_with_changed_companion(self):
        from unittest.mock import patch
        from ucloud_sandboxes.direct_runtime import build_direct_runtime_service

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runsc = self.fixture(root)
            with (
                patch(
                    "ucloud_sandboxes.direct_runtime._cpu_features_sha256",
                    return_value="a" * 64,
                ),
                patch("ucloud_sandboxes.direct_runtime.StorageNativeNodeClient"),
            ):

                def assemble(name):
                    service = build_direct_runtime_service(
                        state_root=root / name,
                        volume_mount_root=root / (name + "-volumes"),
                        runsc=runsc,
                        runsc_commit=GVISOR_COMMIT,
                        init_binary=root / "init",
                        storage_native_socket=root / "storage.sock",
                    )
                    return service.warden.config.runtime_fingerprint

                before = assemble("before")
                (root / "gvisor-bin/gvisor_sentry").write_bytes(b"changed kernel")
                after = assemble("after")
                self.assertEqual(before.runsc_sha256, after.runsc_sha256)
                self.assertNotEqual(before.boot_config_sha256, after.boot_config_sha256)
