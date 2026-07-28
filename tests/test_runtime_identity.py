from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.hibernation import HibernationRuntimeFingerprint
from ucloud_sandboxes.runtime_identity import (
    NodeRuntimeIdentity,
    NodeRuntimeIdentityStore,
    RuntimeIdentityError,
)


class RuntimeIdentityTests(unittest.TestCase):
    def identity(self) -> NodeRuntimeIdentity:
        return NodeRuntimeIdentity(
            runsc_sha256="a" * 64,
            runsc_commit="b" * 40,
            boot_config_sha256="c" * 64,
        )

    def test_first_bind_is_durable_and_exact_replay_is_idempotent(self) -> None:
        with TemporaryDirectory() as raw:
            path = (Path(raw) / "state" / "runtime.json").resolve()
            store = NodeRuntimeIdentityStore(path)
            expected = self.identity()

            self.assertEqual(store.bind(expected), expected)
            self.assertEqual(NodeRuntimeIdentityStore(path).bind(expected), expected)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_mismatched_runtime_cannot_adopt_existing_node_state(self) -> None:
        with TemporaryDirectory() as raw:
            store = NodeRuntimeIdentityStore(
                (Path(raw) / "runtime.json").resolve()
            )
            store.bind(self.identity())

            with self.assertRaisesRegex(RuntimeIdentityError, "another runtime"):
                store.bind(
                    replace(
                        self.identity(),
                        runsc_sha256="d" * 64,
                    )
                )

    def test_symlink_identity_fails_closed(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            target = root / "target"
            target.write_text("{}", encoding="ascii")
            path = root / "runtime.json"
            path.symlink_to(target)

            with self.assertRaisesRegex(RuntimeIdentityError, "regular file"):
                NodeRuntimeIdentityStore(path.absolute()).bind(
                    self.identity()
                )

    def test_identity_derives_only_node_wide_runtime_fields(self) -> None:
        fingerprint = HibernationRuntimeFingerprint(
            runsc_sha256="a" * 64,
            runsc_commit="b" * 40,
            platform="systrap",
            architecture="x86_64",
            page_size=4096,
            cpu_features_sha256="d" * 64,
            boot_config_sha256="c" * 64,
            rootfs_sha256="e" * 64,
        )
        identity = NodeRuntimeIdentity.from_fingerprint(fingerprint)

        self.assertEqual(identity.runsc_sha256, fingerprint.runsc_sha256)
        self.assertEqual(identity.boot_config_sha256, fingerprint.boot_config_sha256)
        self.assertNotIn("rootfs", identity.to_dict())


if __name__ == "__main__":
    unittest.main()
