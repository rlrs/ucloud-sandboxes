#!/usr/bin/env python3
"""Qualify the product Warden's split capture path on an isolated Linux worker.

Requires a pre-mounted XFS project-quota work root, native ublk backend and the
pinned capture-barrier runtime. No production inventory or deployment is used.
"""

from contextlib import contextmanager
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
from unittest.mock import patch

from qualify_volume import Qualifier, GIB
from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.checkpoint_registry import RegistryCheckpointStore
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.managed_registry import RegistryClient
from ucloud_sandboxes.sandbox import (
    SandboxSpec,
    SandboxSecuritySpec,
    sandbox_spec_fingerprint,
)
from ucloud_sandboxes.storage_native_registry import RegistrySnapshotPublisher
from ucloud_sandboxes.environment_manifest import (
    EnvironmentManifest,
    DOCKER_OVERLAY2_ABI,
)
from ucloud_sandboxes.direct_warden import (
    DirectRunscWarden,
    DirectRunscWardenConfig,
    DirectWardenError,
)
from ucloud_sandboxes.hibernation import HibernationRuntimeFingerprint
from ucloud_sandboxes.image_rootfs import (
    DockerImageConfig,
    MaterializedRootfs,
    OverlayRootfsManager,
)
from ucloud_sandboxes.memory_backing import MemoryBackingStore
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeNodeClient,
    StorageNativeNodeConfig,
    StorageNativeNodeService,
    StorageNativeNodeServer,
    StorageVolumeOwner,
)


FINGERPRINT = EnvironmentManifest(base="sha256:" + "a" * 64).rootfs_fingerprint(
    DOCKER_OVERLAY2_ABI
)


class FixtureImageStore:
    """Trusted fixture layer; all overlay/lifecycle behavior is product code."""

    def __init__(self, images):
        self.images = images
        self.image = None

    @contextmanager
    def operation_lease(self, image_ref):
        assert self.image is not None and image_ref == self.image.image_ref
        yield self.image

    def collect_image(self, image_id, *, is_referenced):
        return False

    @contextmanager
    def mounted_rootfs_lease(self, image_id, *, rootfs_identity_sha256):
        assert (
            image_id == "sha256:" + "a" * 64 and rootfs_identity_sha256 == FINGERPRINT
        )
        yield self.images / "rootfs"


def run(args):
    qualifier = Qualifier(
        daemon_binary=args.daemon,
        work_root=args.work_root,
        output=args.output,
        virtual_size=2 * GIB,
        upper_mode="hybridLogStructured",
        runsc=args.runsc,
        conformance_workload=args.conformance_workload,
        noop_workload=args.noop_workload,
        capture_barrier=True,
        filesystem="xfs",
    )
    qualifier._preflight()
    root = qualifier.test_root
    result = {"schema": 1, "status": "failed", "test_root": str(root)}
    server = thread = warden = sandbox = memory = None
    namespace = f"ucloud-split-{os.getpid()}"
    ram_root = root / "ram-active" if args.ram_active else None
    try:
        if ram_root is not None:
            ram_root.mkdir(mode=0o700)
            subprocess.run(
                [
                    "mount",
                    "-t",
                    "tmpfs",
                    "-o",
                    f"size={args.memory_mb * 3}m,noswap,mode=0700",
                    "ucloud-memory-qualification",
                    str(ram_root),
                ],
                check=True,
            )
        qualifier._start_daemon()
        mounts = root / "mounts"
        mounts.mkdir(mode=0o700)
        service = StorageNativeNodeService(
            StorageNativeNodeConfig(
                journal_path=root / "storage.sqlite",
                runtime_root=root / "volumes",
                mount_root=mounts,
                hard_capacity_bytes=16 * GIB,
            ),
            backend=qualifier.client,
            global_config_path=root / "global.json",
        )
        server = StorageNativeNodeServer(root / "storage.sock", service)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = StorageNativeNodeClient(root / "storage.sock")
        client.wait_ready()
        memory = MemoryBackingStore(
            mounts,
            root / "memory.sqlite",
            hard_capacity_bytes=16 * GIB,
            active_root=ram_root,
        )
        # Actual kernel quota enforcement, then explicit allocation cleanup.
        quota_ref = MemoryBackingRef("quota.sandbox-1", 1024 * 1024)
        lease = memory.prepare(quota_ref, sandbox_id="quota", sandbox_generation=1)
        try:
            fd = os.open(lease.path / "exhaust", os.O_WRONLY | os.O_CREAT, 0o600)
            try:
                os.posix_fallocate(fd, 0, 2 * 1024 * 1024)
                raise AssertionError("project hard quota was not enforced")
            except OSError as exc:
                if exc.errno not in {errno.EDQUOT, errno.ENOSPC}:
                    raise
                result["project_quota_errno"] = exc.errno
            finally:
                os.close(fd)
        finally:
            memory.delete(quota_ref, sandbox_id="quota", sandbox_generation=1)
        incarnation = "qualifier.sandbox-1"
        spec = SandboxSpec(
            id="qualifier",
            image="fixture",
            memory_mb=args.memory_mb,
            disk_mb=2048,
            cpus=1.0,
            parkable=True,
            network="none",
            command=("/conformance-workload", "server"),
            security=SandboxSecuritySpec(init=False),
        )
        ref = MemoryBackingRef(
            incarnation, (spec.requested_resources().disk_mb - spec.disk_mb) * 1024**2
        )
        memory.prepare(ref, sandbox_id="qualifier", sandbox_generation=1)
        owner = StorageVolumeOwner("workspace-" + incarnation, "qualifier", 1)
        client.prepare_volume(owner, operation_id="create", virtual_size=2 * GIB)
        images = root / "images"
        lower = images / "rootfs"
        lower.mkdir(parents=True)
        for directory in ("dev", "proc", "run", "sys", "tmp"):
            (lower / directory).mkdir()
        for source, name in (
            (args.conformance_workload, "conformance-workload"),
            (args.noop_workload, "noop"),
        ):
            shutil.copyfile(source, lower / name)
            (lower / name).chmod(0o755)
        image = MaterializedRootfs(
            "fixture", "sha256:" + "a" * 64, FINGERPRINT, lower, DockerImageConfig()
        )
        overlays = OverlayRootfsManager(
            FixtureImageStore(images),
            writable_root=mounts,
            bundle_root=root / "bundles",
            require_precreated_writable=True,
        )
        subprocess.run(["ip", "netns", "add", namespace], check=True)
        subprocess.run(
            ["ip", "netns", "exec", namespace, "ip", "link", "set", "lo", "up"],
            check=True,
        )
        config = qualifier._gvisor_config(namespace, incarnation)
        config["linux"]["resources"]["memory"]["limit"] = args.memory_mb * 1024**2
        config["linux"]["resources"]["memory"]["swap"] = args.memory_mb * 1024**2
        lease = overlays.prepare(
            sandbox_id="qualifier",
            sandbox_generation=1,
            image=image,
            config_template=config,
            spec_sha256=sandbox_spec_fingerprint(spec),
            workspace_directory=owner.volume_id,
            memory=ref,
        )
        sandbox = lease.sandbox
        runtime = HibernationRuntimeFingerprint(
            runsc_sha256=hashlib.sha256(args.runsc.read_bytes()).hexdigest(),
            runsc_commit="50e1502a95d36ad2faf2c7ef33b8bf21fe975293",
            platform="systrap",
            architecture=os.uname().machine,
            page_size=os.sysconf("SC_PAGE_SIZE"),
            cpu_features_sha256="c" * 64,
            boot_config_sha256="d" * 64,
            rootfs_sha256="a" * 64,
        )
        capacity = DirectSandboxRegistry(
            root / "capacity.sqlite", hard_disk_capacity_mb=16384
        )
        registration = capacity.plan(
            spec=spec, sandbox_generation=1, operation_id="create",
            runtime_compatibility_sha256=runtime.node_compatibility_sha256,
            split_memory_backing=True,
        )
        volume = client.get_volume(owner.volume_id)
        registration = capacity.commit_quota(
            spec.id, expected_revision=registration.revision,
            project_id=volume.accounting_id,
            total_mb=spec.requested_resources().disk_mb,
            quota_path=Path(volume.mount_path),
        )
        registration = capacity.commit_rootfs(
            spec.id, expected_revision=registration.revision,
            image_id=image.image_id, sandbox=sandbox,
        )
        capacity.commit_owned(spec.id, expected_revision=registration.revision)
        warden = DirectRunscWarden(
            DirectRunscWardenConfig(
                runsc=args.runsc,
                runtime_root=root / "runsc",
                memory_root=mounts,
                application_memory_root=ram_root,
                reflink_memory_restore=args.reflink_restore,
                bundle_root=root / "bundles",
                journal_root=root / "journals",
                runtime_fingerprint=runtime,
                network="none" if args.registry_binary else "sandbox",
                readiness_command=("/noop",),
            ),
            storage=client,
            rootfs_lifecycle=overlays,
            memory_backing=memory,
            memory_capacity=capacity,
        )
        initial = warden.create(sandbox, operation_id="create")

        def memory_charge():
            pid = warden._journal(sandbox).load().sentry_pid
            line = Path(f"/proc/{pid}/cgroup").read_text().strip()
            assert line.startswith("0::/")
            group = Path("/sys/fs/cgroup") / line[4:]
            stats = dict(
                line.split()
                for line in (group / "memory.stat").read_text().splitlines()
            )
            value = {
                "current": int((group / "memory.current").read_text()),
                "limit": int((group / "memory.max").read_text()),
                "shmem": int(stats["shmem"]),
                "cgroup": str(group),
            }
            assert value["limit"] == args.memory_mb * 1024**2
            value["active_mode"] = warden.application_memory_mode(sandbox.sandbox_id, sandbox.sandbox_generation)
            if value["active_mode"] == "ram":
                assert value["shmem"] >= 16 * 1024**2, value
                assert value["current"] >= value["shmem"], value
            return value

        def check(command="client"):
            response = warden._checked(
                *warden._state_prefix(),
                "exec",
                sandbox.container_id,
                "/conformance-workload",
                command,
            )
            if not response.stdout.strip().startswith("ok "):
                raise AssertionError(response.stdout)

        ready_started = time.monotonic()
        while True:
            try:
                check()
                break
            except DirectWardenError:
                if (
                    time.monotonic() - ready_started >= 30
                    or not warden.running_process_alive(sandbox)
                ):
                    raise
                time.sleep(0.05)
        result["workload_ready_seconds"] = time.monotonic() - ready_started
        if args.qualify_resident_wait or args.resident_wait_only:
            from qualify_resident_wait import qualify_live_wait

            result["resident_wait"] = qualify_live_wait(
                warden,
                sandbox,
                check,
                target_bytes=256 * 1024**2,
                compare_pause=args.compare_pause,
            )
            if args.resident_wait_only:
                result.update(status="passed", quota=memory.metrics())
                return
        # A failed commit must return to this exact original runtime, including
        # netstack, timers, persistent file descriptors and anonymous pages.
        with patch.object(
            warden.artifacts,
            "publish_complete",
            side_effect=OSError("qualification injected precommit failure"),
        ):
            try:
                warden.park(sandbox, operation_id="abort")
            except OSError:
                pass
            else:
                raise AssertionError("injected capture unexpectedly committed")
        check()
        aborted = warden._journal(sandbox).load()
        assert (initial.sentry_pid, initial.sentry_start_time_ticks) == (
            aborted.sentry_pid,
            aborted.sentry_start_time_ticks,
        )
        result["abort_same_original"] = True
        cycles = []
        for number in range(3):
            if args.dirty_command:
                check(args.dirty_command)
            parked = warden.park(sandbox, operation_id=f"park-{number}")
            manifest = warden.artifacts.load_complete(
                sandbox_id="qualifier",
                sandbox_generation=1,
                hibernation_generation=parked.hibernation_generation,
            )
            assert manifest.version == 3 and manifest.memory == ref
            assert (
                client.get_volume(owner.volume_id).capture_id
                == manifest.workspace.capture_id
            )
            timings = {}
            before = time.monotonic()
            warden.resume(sandbox, operation_id=f"wake-{number}", timings=timings)
            elapsed = time.monotonic() - before
            check()
            live_reclaim = None
            if args.reflink_restore:
                live_before = warden._journal(sandbox).load()
                charge_before = memory_charge()
                started = time.monotonic()
                assert warden.flush_reclaimable_memory(sandbox)
                writeback_seconds = time.monotonic() - started
                started = time.monotonic()
                try:
                    (Path(charge_before["cgroup"]) / "memory.reclaim").write_text(
                        f"{args.memory_mb * 1024**2} swappiness=0"
                    )
                except OSError as exc:
                    if exc.errno != errno.EAGAIN:
                        raise
                reclaim_seconds = time.monotonic() - started
                charge_after = memory_charge()
                started = time.monotonic()
                check()
                check_seconds = time.monotonic() - started
                assert live_before == warden._journal(sandbox).load()
                live_reclaim = {
                    "writeback_seconds": writeback_seconds,
                    "reclaim_seconds": reclaim_seconds,
                    "full_integrity_seconds": check_seconds,
                    "before": charge_before, "after": charge_after,
                    "same_live_owner": True,
                }
            cycles.append(
                {
                    "wake_seconds": elapsed,
                    "live_reclaim": live_reclaim,
                    "timings_ms": timings,
                    "capture_id": manifest.workspace.capture_id,
                    "memory_charge": memory_charge(),
                }
            )
        if args.qualify_ram_limit:
            if not args.ram_active or args.memory_mb < 1024:
                raise ValueError(
                    "RAM limit qualification needs >=1GiB with large workload"
                )
            warden.park(sandbox, operation_id="ram-limit-park")
            config_path = sandbox.bundle / "config.json"
            original_config = config_path.read_text()
            limited_config = json.loads(original_config)
            limited_config["linux"]["resources"]["memory"] = {
                "limit": 128 * 1024**2,
                "swap": 128 * 1024**2,
            }
            config_path.write_text(json.dumps(limited_config))
            group = Path("/sys/fs/cgroup") / limited_config["linux"][
                "cgroupsPath"
            ].lstrip("/")
            stop = threading.Event()
            observations = []

            def observe_limit():
                while not stop.wait(0.005):
                    try:
                        events = dict(
                            line.split()
                            for line in (group / "memory.events")
                            .read_text()
                            .splitlines()
                        )
                        observations.append(
                            {
                                "oom_kill": int(events["oom_kill"]),
                                "current": int((group / "memory.current").read_text()),
                                "limit": int((group / "memory.max").read_text()),
                            }
                        )
                    except (OSError, ValueError):
                        pass

            monitor = threading.Thread(target=observe_limit)
            monitor.start()
            try:
                try:
                    warden.resume(sandbox, operation_id="ram-limit-wake")
                except Exception as exc:
                    failure = type(exc).__name__
                else:
                    raise AssertionError("restored RAM escaped sandbox memory limit")
            finally:
                stop.set()
                monitor.join()
                config_path.write_text(original_config)
            assert any(item["oom_kill"] > 0 for item in observations), observations
            assert any(item["limit"] == 128 * 1024**2 for item in observations), (
                observations
            )
            warden.reconcile(sandbox)
            warden.resume(sandbox, operation_id="ram-limit-retry")
            check()
            result["ram_limit"] = {
                "status": "passed",
                "failure": failure,
                "oom_kill": max(item["oom_kill"] for item in observations),
                "retry_charge": memory_charge(),
            }
        if args.registry_binary is not None:
            result["remote_import"] = qualify_remote(
                args, qualifier, root, service, warden, sandbox, overlays, image, spec
            )
        result.update(status="passed", cycles=cycles, quota=memory.metrics())
    except BaseException as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            if warden is not None and sandbox is not None:
                warden.delete(sandbox)
                overlays.release_sandbox(sandbox)
                client.delete_volume(
                    owner,
                    operation_id="qualification-delete",
                    expected_virtual_size=2 * GIB,
                )
                memory.delete(ref, sandbox_id="qualifier", sandbox_generation=1)
                warden.release_deleted_memory_capacity(sandbox)
                result["cleanup_quota"] = memory.metrics()
                assert result["cleanup_quota"]["memory_backing_hard_reserved_bytes"] == 0
                assert not capacity.list_reflink_overlaps("qualifier", 1)
            if server:
                server.shutdown()
            if thread:
                thread.join(timeout=5)
            subprocess.run(["ip", "netns", "delete", namespace], capture_output=True)
            qualifier._cleanup()
            if ram_root is not None:
                subprocess.run(["umount", str(ram_root)], check=True)
        except BaseException as exc:
            result["cleanup_error"] = f"{type(exc).__name__}: {exc}"
            result["status"] = "failed"
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


def qualify_remote(
    args,
    qualifier,
    root,
    source_storage,
    source_warden,
    source_sandbox,
    source_overlays,
    image,
    spec,
):
    """Real OCI registry, independent node stores and the canonical import API."""
    with socket.socket() as port_probe:
        port_probe.bind(("127.0.0.1", 0))
        port = port_probe.getsockname()[1]
    registry_config = root / "registry.json"
    registry_config.write_text(
        json.dumps(
            {
                "version": 0.1,
                "storage": {
                    "filesystem": {"rootdirectory": str(root / "registry-data")},
                    "delete": {"enabled": True},
                },
                "http": {"addr": f"127.0.0.1:{port}"},
                "log": {"level": "error"},
            }
        )
    )
    log = (root / "registry.log").open("w")
    registry_process = subprocess.Popen(
        [str(args.registry_binary), "serve", str(registry_config)],
        stdout=log,
        stderr=log,
    )
    registry = RegistryClient(f"http://127.0.0.1:{port}")
    backend = None
    destination = None
    stream_root = Path(f"/tmp/p3-checkpoint-streams-{os.getpid()}")
    try:
        for attempt in range(100):
            try:
                registry.catalog()
                break
            except Exception:
                if registry_process.poll() is not None:
                    raise RuntimeError("isolated registry exited")
                time.sleep(0.05)
        else:
            raise RuntimeError("isolated registry did not become ready")
        store = RegistryCheckpointStore(
            registry, repository="qualification/checkpoints"
        )
        publisher = RegistrySnapshotPublisher(
            registry,
            repository="qualification/checkpoints",
            stream_socket_root=stream_root,
        )
        source_storage.publisher = publisher
        source_registry = DirectSandboxRegistry(
            root / "source-registry.sqlite", hard_disk_capacity_mb=8192
        )
        registration = source_registry.plan(
            spec=spec,
            sandbox_generation=1,
            operation_id="create",
            runtime_compatibility_sha256=source_warden.config.runtime_fingerprint.node_compatibility_sha256,
            split_memory_backing=True,
        )
        volume = source_warden.storage.get_volume(source_sandbox.workspace_directory)
        registration = source_registry.commit_quota(
            spec.id,
            expected_revision=registration.revision,
            project_id=volume.accounting_id,
            total_mb=spec.requested_resources().disk_mb,
            quota_path=Path(volume.mount_path),
        )
        registration = source_registry.commit_rootfs(
            spec.id,
            expected_revision=registration.revision,
            image_id=image.image_id,
            sandbox=source_sandbox,
        )
        registration = source_registry.commit_owned(
            spec.id, expected_revision=registration.revision
        )
        source_overlays.image_store.image = image

        class FixtureOci(DirectOciConfigBuilder):
            def build(self, *_args, **_kwargs):
                payload = json.loads(
                    (source_sandbox.bundle / "config.json").read_text()
                )
                payload["linux"]["cgroupsPath"] = (
                    f"/ucloud-split-destination-{os.getpid()}/{source_sandbox.container_id}"
                )
                return payload

        source = DirectSandboxProvisioner(
            registry=source_registry,
            overlays=source_overlays,
            oci=FixtureOci(),
            warden=source_warden,
            checkpoint_store=store,
        )
        source_warden.park(source_sandbox, operation_id="publish-remote")
        service = DirectSandboxService(source)
        migration = service._split_storage_native_snapshot(registration)
        store.verify_root(
            migration.reference,
            migration.publication,
            migration.memory_publication,
            portable_manifest=migration.manifest.to_dict(),
        )
        destination_root = root / "destination"
        destination_root.mkdir(mode=0o700)
        mounts = destination_root / "mounts"
        mounts.mkdir(mode=0o700)
        dst_storage = StorageNativeNodeService(
            StorageNativeNodeConfig(
                journal_path=destination_root / "storage.sqlite",
                runtime_root=destination_root / "volumes",
                mount_root=mounts,
                hard_capacity_bytes=16 * GIB,
            ),
            backend=qualifier.client,
            global_config_path=root / "global.json",
            publisher=publisher,
        )
        backend = StorageNativeNodeServer(
            stream_root / "destination-storage.sock", dst_storage
        )
        thread = threading.Thread(target=backend.serve_forever, daemon=True)
        thread.start()
        client = StorageNativeNodeClient(stream_root / "destination-storage.sock")
        client.wait_ready()
        dst_active_root = None
        if source_warden.config.application_memory_root is not None:
            dst_active_root = (
                source_warden.config.application_memory_root / "destination"
            )
            dst_active_root.mkdir(mode=0o700)
        dst_memory = MemoryBackingStore(
            mounts,
            destination_root / "memory.sqlite",
            hard_capacity_bytes=16 * GIB,
            active_root=dst_active_root,
        )
        dst_images = FixtureImageStore(source_overlays.image_store.images)
        dst_images.image = image
        dst_overlays = OverlayRootfsManager(
            dst_images,
            writable_root=mounts,
            bundle_root=destination_root / "bundles",
            require_precreated_writable=True,
        )
        from dataclasses import replace

        dst_warden = DirectRunscWarden(
            replace(
                source_warden.config,
                runtime_root=destination_root / "runsc",
                memory_root=mounts,
                application_memory_root=dst_active_root,
                bundle_root=destination_root / "bundles",
                journal_root=destination_root / "journals",
            ),
            storage=client,
            rootfs_lifecycle=dst_overlays,
            memory_backing=dst_memory,
        )
        destination = DirectSandboxProvisioner(
            registry=DirectSandboxRegistry(
                destination_root / "registry.sqlite", hard_disk_capacity_mb=8192
            ),
            overlays=dst_overlays,
            oci=FixtureOci(),
            warden=dst_warden,
            checkpoint_store=store,
        )
        imported, rebound = destination.stage_storage_native_import(
            migration, migration_id="qualification-import"
        )
        assert rebound.reference == migration.reference
        imported = destination.activate_import(
            spec.id,
            migration_id="qualification-import",
            migration_sha256=migration.sha256,
        )
        timings = {}
        dst_warden.resume(
            imported.to_direct_sandbox(), operation_id="remote-wake", timings=timings
        )
        response = dst_warden._checked(
            *dst_warden._state_prefix(),
            "exec",
            imported.container_id,
            "/conformance-workload",
            "client",
        )
        if not response.stdout.strip().startswith("ok "):
            raise AssertionError(response.stdout)
        return {
            "status": "passed",
            "root_manifest": migration.reference.manifest_digest,
            "memory_manifest": migration.memory_publication.reference.manifest_digest,
            "workspace_manifest": migration.publication.manifest_digest,
            "wake_timings_ms": timings,
        }
    finally:
        if destination is not None:
            destination.delete(spec.id)
        if backend is not None:
            backend.shutdown()
        registry_process.terminate()
        registry_process.wait(timeout=10)
        log.close()
        shutil.rmtree(stream_root, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in (
        "daemon",
        "work-root",
        "runsc",
        "conformance-workload",
        "noop-workload",
        "output",
    ):
        parser.add_argument("--" + arg, type=Path, required=True)
    parser.add_argument("--registry-binary", type=Path)
    parser.add_argument("--ram-active", action="store_true")
    parser.add_argument("--reflink-restore", action="store_true")
    parser.add_argument("--dirty-command")
    parser.add_argument("--qualify-ram-limit", action="store_true")
    parser.add_argument("--compare-pause", action="store_true")
    parser.add_argument("--memory-mb", type=int, default=512)
    parser.add_argument("--qualify-resident-wait", action="store_true")
    parser.add_argument("--resident-wait-only", action="store_true")
    run(parser.parse_args())
