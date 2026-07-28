from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.direct_registry import (
    DirectRegistryConflictError,
    DirectSandboxRegistry,
)
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.sandbox import SandboxSpec


class DirectRegistryTests(unittest.TestCase):
    def spec(self, sandbox_id: str = "sandbox") -> SandboxSpec:
        return SandboxSpec(
            id=sandbox_id,
            image="registry/image@sha256:" + "a" * 64,
            memory_mb=1024,
            disk_mb=2048,
        )

    def test_registration_survives_every_provisioning_boundary(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.json").resolve()
            registry = DirectSandboxRegistry(path)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            quota = registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=200_000,
                total_mb=4096,
                quota_path=(root / "quota" / "sandbox.sandbox-7").resolve(),
            )
            sandbox = DirectSandbox(
                sandbox_id="sandbox",
                sandbox_generation=7,
                container_id="c" * 64,
                spec_sha256=quota.spec_sha256,
                rootfs_sha256="d" * 64,
                bundle=(root / "bundles" / "sandbox.sandbox-7").resolve(),
                memory_directory="sandbox.sandbox-7",
            )
            rootfs = registry.commit_rootfs(
                "sandbox",
                expected_revision=quota.revision,
                image_id="sha256:" + "e" * 64,
                sandbox=sandbox,
            )
            owned = registry.commit_owned(
                "sandbox",
                expected_revision=rootfs.revision,
            )

            reopened = DirectSandboxRegistry(path).get("sandbox")
            self.assertEqual(reopened, owned)
            assert reopened is not None
            self.assertEqual(reopened.to_direct_sandbox(), sandbox)

    def test_exact_plan_replay_is_idempotent_but_mismatch_conflicts(self) -> None:
        with TemporaryDirectory() as raw:
            registry = DirectSandboxRegistry(
                (Path(raw) / "registry.json").resolve()
            )
            first = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            replay = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            self.assertEqual(first, replay)
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "another direct registration",
            ):
                registry.plan(
                    spec=self.spec(),
                    sandbox_generation=8,
                    operation_id="create:8",
                    runtime_identity_sha256="b" * 64,
                )

    def test_delete_tombstone_fences_delayed_create(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            registry = DirectSandboxRegistry(
                (root / "registry.json").resolve()
            )
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            quota = registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=200_000,
                total_mb=4096,
                quota_path=(root / "quota" / "sandbox.sandbox-7").resolve(),
            )
            rootfs = registry.commit_rootfs(
                "sandbox",
                expected_revision=quota.revision,
                image_id="sha256:" + "e" * 64,
                sandbox=DirectSandbox(
                    sandbox_id="sandbox",
                    sandbox_generation=7,
                    container_id="c" * 64,
                    spec_sha256=quota.spec_sha256,
                    rootfs_sha256="d" * 64,
                    bundle=(root / "bundle").resolve(),
                    memory_directory="sandbox.sandbox-7",
                ),
            )
            owned = registry.commit_owned(
                "sandbox",
                expected_revision=rootfs.revision,
            )
            deleting = registry.begin_delete(
                "sandbox",
                expected_revision=owned.revision,
            )
            registry.commit_deleted(
                "sandbox",
                sandbox_generation=7,
                expected_revision=deleting.revision,
            )

            self.assertEqual(registry.list(), ())
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "tombstone",
            ):
                registry.plan(
                    spec=self.spec(),
                    sandbox_generation=7,
                    operation_id="create:7-retry",
                    runtime_identity_sha256="b" * 64,
                )

    def test_fork_is_explicitly_deferred(self) -> None:
        with TemporaryDirectory() as raw:
            registry = DirectSandboxRegistry(
                (Path(raw) / "registry.json").resolve()
            )
            with self.assertRaisesRegex(ValueError, "fork is deferred"):
                registry.plan(
                    spec=SandboxSpec(
                        id="fork",
                        image="image",
                        memory_mb=1024,
                        disk_mb=1024,
                        forkable=True,
                    ),
                    sandbox_generation=1,
                    operation_id="create:1",
                    runtime_identity_sha256="b" * 64,
                )


if __name__ == "__main__":
    unittest.main()
