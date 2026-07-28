from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from threading import Thread
from urllib import request
import json
import shutil
import unittest

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_node_adapter import (
    DirectNodeManagerAdapter,
    DirectNodeStateStore,
)
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_service import DirectExecResult, DirectSandboxService
from ucloud_sandboxes.direct_registry import DirectRegistryError, DirectSandboxRegistry
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.hibernation import (
    HibernationDiskLedger,
    HibernationRuntimeFingerprint,
    HibernationState,
)
from ucloud_sandboxes.image_rootfs import (
    DockerImageConfig,
    MaterializedRootfs,
    OverlayRootfsLease,
)
from ucloud_sandboxes.runtime_identity import NodeRuntimeIdentityStore
from ucloud_sandboxes.node_agent import build_direct_node_agent_server
from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec


class FakeImageStore:
    def __init__(self, root: Path) -> None:
        rootfs = root / "image-rootfs"
        rootfs.mkdir()
        self.image = MaterializedRootfs(
            image_ref="image",
            image_id="sha256:" + "a" * 64,
            rootfs_identity_sha256="b" * 64,
            rootfs=rootfs,
            image_config=DockerImageConfig(command=("sleep", "3600")),
        )
        self.reconciled = False

    def materialize(self, image_ref: str) -> MaterializedRootfs:
        if image_ref != "image":
            raise AssertionError("wrong image")
        return self.image

    def reconcile_export_containers(self) -> tuple[str, ...]:
        self.reconciled = True
        return ()


class FakeQuota:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.items: dict[tuple[str, int], dict] = {}

    def prepare(self, reservation):
        path = self.root / (
            f"{reservation.sandbox_id}.sandbox-{reservation.sandbox_generation}"
        )
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        payload = {
            "hard_limit_mb": reservation.total_mb,
            "path": str(path),
            "project_id": reservation.project_id,
            "sandbox_generation": reservation.sandbox_generation,
            "sandbox_id": reservation.sandbox_id,
            "state": "ready",
        }
        self.items[(reservation.sandbox_id, reservation.sandbox_generation)] = payload
        return payload

    def drop(self, reservation):
        key = (reservation.sandbox_id, reservation.sandbox_generation)
        payload = self.items.pop(key, None)
        if payload is not None:
            shutil.rmtree(payload["path"])
        return {
            "project_id": reservation.project_id,
            "removed": payload is not None,
            "sandbox_generation": reservation.sandbox_generation,
            "sandbox_id": reservation.sandbox_id,
            "state": "absent",
        }

    def inventory(self):
        return tuple(self.items.values())


class FakeOverlays:
    require_precreated_writable = True

    def __init__(self, image_store: FakeImageStore, root: Path) -> None:
        self.image_store = image_store
        self.writable_root = root / "quota"
        self.bundle_root = root / "bundles"
        self.writable_root.mkdir()
        self.bundle_root.mkdir()

    def discard_unregistered(self, *, sandbox_id, sandbox_generation):
        bundle = self.bundle_root / f"{sandbox_id}.sandbox-{sandbox_generation}"
        if bundle.exists():
            shutil.rmtree(bundle)

    def prepare(
        self,
        *,
        sandbox_id,
        sandbox_generation,
        image_ref,
        config_template,
        spec_sha256,
    ):
        del config_template
        image = self.image_store.materialize(image_ref)
        incarnation = f"{sandbox_id}.sandbox-{sandbox_generation}"
        writable = self.writable_root / incarnation
        if not writable.is_dir():
            raise AssertionError("quota was not prepared first")
        upper = writable / "upper"
        work = writable / "work"
        upper.mkdir()
        work.mkdir()
        bundle = self.bundle_root / incarnation
        merged = bundle / "rootfs"
        merged.mkdir(parents=True)
        sandbox = DirectSandbox(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            container_id="c" * 64,
            spec_sha256=spec_sha256,
            rootfs_sha256=image.rootfs_identity_sha256,
            bundle=bundle,
            memory_directory=incarnation,
        )
        return OverlayRootfsLease(
            sandbox=sandbox,
            image=image,
            writable=writable,
            upper=upper,
            work=work,
            merged=merged,
            writable_owned_by_manager=False,
        )

    def release_sandbox(self, sandbox):
        if sandbox.bundle.exists():
            shutil.rmtree(sandbox.bundle)


class FakeWarden:
    def __init__(self, root: Path) -> None:
        fingerprint = HibernationRuntimeFingerprint(
            runsc_sha256="d" * 64,
            runsc_commit="e" * 40,
            platform="systrap",
            architecture="x86_64",
            page_size=4096,
            cpu_features_sha256="f" * 64,
            boot_config_sha256="1" * 64,
            rootfs_sha256="2" * 64,
        )
        quota = root / "quota"
        bundles = root / "bundles"
        self.config = SimpleNamespace(
            artifact_root=quota,
            bundle_root=bundles,
            memory_root=quota,
            network="none",
            remove_memory_directory_on_delete=False,
            runtime_fingerprint=fingerprint,
        )
        self.records = {}
        self.discarded = []

    @staticmethod
    def key(sandbox):
        return sandbox.sandbox_id, sandbox.sandbox_generation

    def inspect(self, sandbox):
        return self.records.get(self.key(sandbox))

    def discard_unjournaled(self, sandbox):
        if self.inspect(sandbox) is not None:
            raise AssertionError("journal already exists")
        self.discarded.append(self.key(sandbox))

    def create(self, sandbox, *, operation_id):
        del operation_id
        record = SimpleNamespace(state=HibernationState.RUNNING)
        self.records[self.key(sandbox)] = record
        return record

    def reconcile(self, sandbox):
        return self.records[self.key(sandbox)]

    def park(self, sandbox, *, operation_id):
        del operation_id
        record = SimpleNamespace(state=HibernationState.PARKED)
        self.records[self.key(sandbox)] = record
        return record

    def resume(self, sandbox, *, operation_id, timings=None):
        del operation_id
        if timings is not None:
            timings["runsc_restore"] = 1.0
        record = SimpleNamespace(state=HibernationState.RUNNING)
        self.records[self.key(sandbox)] = record
        return record

    @contextmanager
    def exec_lease(self, sandbox, argv, *, env=None, working_dir=None, user=None):
        del env, working_dir, user
        yield ("runsc", "exec", sandbox.container_id, *argv)

    def delete(self, sandbox):
        self.records.pop(self.key(sandbox), None)


class FakeProcessRunner:
    def __init__(self) -> None:
        self.calls = []

    def run(
        self,
        argv,
        *,
        input_bytes,
        timeout_seconds,
        max_stdout_bytes,
        max_stderr_bytes,
    ):
        del timeout_seconds, max_stdout_bytes, max_stderr_bytes
        self.calls.append((tuple(argv), input_bytes))
        return DirectExecResult(tuple(argv), 0, b"ok\n", b"")


class DirectProvisionerTests(unittest.TestCase):
    def make(self, root: Path):
        images = FakeImageStore(root)
        overlays = FakeOverlays(images, root)
        quota = FakeQuota(overlays.writable_root)
        warden = FakeWarden(root)
        registry = DirectSandboxRegistry((root / "registry.json").resolve())
        ledger = HibernationDiskLedger(
            (root / "ledger.json").resolve(),
            capacity_mb=100_000,
            safety_headroom_mb=1_000,
        )
        provisioner = DirectSandboxProvisioner(
            identity_store=NodeRuntimeIdentityStore(
                (root / "runtime-identity.json").resolve()
            ),
            registry=registry,
            disk_ledger=ledger,
            quota_backend=quota,
            image_store=images,
            overlays=overlays,
            oci=DirectOciConfigBuilder(),
            warden=warden,
        )
        return provisioner, registry, ledger, quota, images, warden

    @staticmethod
    def spec() -> SandboxSpec:
        return SandboxSpec(
            id="sandbox",
            image="image",
            memory_mb=1024,
            disk_mb=2048,
            security=SandboxSecuritySpec(init=False),
        )

    def test_create_and_delete_order_all_owners(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, ledger, quota, _, warden = self.make(root)

            created = provisioner.create(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
            )

            self.assertEqual(created.registration.phase, "owned")
            self.assertEqual(created.lifecycle_state, HibernationState.RUNNING)
            self.assertEqual(len(ledger.inventory().reservations), 1)
            self.assertEqual(len(quota.inventory()), 1)
            self.assertIn(("sandbox", 7), warden.discarded)

            provisioner.delete("sandbox")

            self.assertIsNone(registry.get("sandbox"))
            self.assertEqual(ledger.inventory().reservations, ())
            self.assertEqual(quota.inventory(), ())

    def test_restart_advances_quota_ready_registration(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, ledger, quota, images, _ = self.make(root)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=9,
                operation_id="create:9",
                runtime_identity_sha256=provisioner.identity.digest,
            )
            reservation = ledger.reserve(
                sandbox_id="sandbox",
                sandbox_generation=9,
                memory_mb=1024,
                writable_disk_mb=2048,
            )
            payload = quota.prepare(reservation)
            registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=reservation.project_id,
                total_mb=reservation.total_mb,
                quota_path=Path(payload["path"]),
            )

            results = provisioner.start()

            self.assertTrue(images.reconciled)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].registration.phase, "owned")

    def test_start_fails_closed_on_orphan_capacity_owner(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, ledger, _, _, _ = self.make(root)
            ledger.reserve(
                sandbox_id="orphan",
                sandbox_generation=1,
                memory_mb=1024,
                writable_disk_mb=1024,
            )

            with self.assertRaisesRegex(DirectRegistryError, "absent"):
                provisioner.start()

    def test_service_wakes_for_exec_and_supports_binary_file_input(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            runner = FakeProcessRunner()
            service = DirectSandboxService(provisioner, process_runner=runner)
            created = service.create(self.spec())
            service.park(created.spec.id)

            result = service.exec(created.spec.id, ("/bin/echo", "ok"))
            service.write_file(created.spec.id, "/workspace/payload", b"\0binary")

            self.assertEqual(result.stdout, b"ok\n")
            self.assertEqual(
                service.get(created.spec.id).state,
                HibernationState.RUNNING.value,
            )
            self.assertEqual(runner.calls[-1][1], b"\0binary")

    def test_node_adapter_holds_exec_lease_and_accounts_parked_memory(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())
            manager = DirectNodeManagerAdapter(service)
            service.park(created.spec.id)

            manager.lifecycle.acquire_shared(created.spec.id)
            command = manager.runtime.exec_command(
                created.spec.id,
                ("/bin/true",),
                interactive=False,
            )
            manager.lifecycle.release_shared(created.spec.id)
            snapshot = manager.heartbeat_snapshot(active_build_count=lambda: 0)

            self.assertEqual(command[:2], ("runsc", "exec"))
            self.assertEqual(snapshot.activity.active_sandboxes, 1)
            self.assertEqual(snapshot.activity.used_resources.memory_mb, 1024)
            service.park(created.spec.id)
            parked = manager.heartbeat_snapshot(active_build_count=lambda: 0)
            self.assertEqual(parked.activity.active_sandboxes, 0)
            self.assertEqual(parked.activity.used_resources.memory_mb, 0)

    def test_direct_node_drain_survives_adapter_restart(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            state_store = DirectNodeStateStore(root / "direct-node-state.json")
            manager = DirectNodeManagerAdapter(service, state_store=state_store)

            drained = manager.configure_drain(
                "drain-test",
                True,
                active_build_count=lambda: 0,
            )
            restarted = DirectNodeManagerAdapter(
                service,
                state_store=DirectNodeStateStore(root / "direct-node-state.json"),
            )
            restarted_snapshot = restarted.heartbeat_snapshot(
                active_build_count=lambda: 0
            )
            restarted_admission_open = service.admission_open
            opened = restarted.configure_drain(
                "drain-test",
                False,
                active_build_count=lambda: 0,
            )

            self.assertTrue(drained.drain.draining)
            self.assertTrue(restarted_snapshot.drain.draining)
            self.assertFalse(restarted_admission_open)
            self.assertFalse(opened.drain.draining)
            self.assertTrue(service.admission_open)

    def test_direct_node_server_binds_only_direct_manager(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )

            server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=service,
                image_file=root / "images.json",
                job_id="job",
                node_id="node",
            )
            try:
                self.assertIs(server.RequestHandlerClass.manager.service, service)
                self.assertIn(
                    "direct-runsc-v1",
                    server.RequestHandlerClass.capabilities,
                )
            finally:
                server.server_close()

    def test_direct_node_park_endpoint_and_exec_wake(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())
            server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=service,
                image_file=root / "images.json",
                job_id="job",
                node_id="node",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                park_request = request.Request(
                    f"http://{host}:{port}/v1/sandboxes/{created.spec.id}/park",
                    data=json.dumps({"operation_id": "park:test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(park_request) as response:
                    parked = json.load(response)["sandbox"]
                self.assertEqual(parked["state"], HibernationState.PARKED.value)

                manager = server.RequestHandlerClass.manager
                manager.lifecycle.acquire_shared(created.spec.id)
                try:
                    manager.runtime.exec_command(
                        created.spec.id,
                        ("/bin/true",),
                        interactive=False,
                    )
                finally:
                    manager.lifecycle.release_shared(created.spec.id)
                self.assertEqual(
                    service.get(created.spec.id).state,
                    HibernationState.RUNNING.value,
                )
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=1)


if __name__ == "__main__":
    unittest.main()
