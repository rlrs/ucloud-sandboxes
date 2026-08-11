#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from ucloud_sandboxes.direct_warden import (  # noqa: E402
    DirectRunscWarden,
    DirectRunscWardenConfig,
)
from ucloud_sandboxes.storage_native_migration import (  # noqa: E402
    MIGRATION_CONNECTION_POLICY_NONE,
    StorageNativeArtifactFile,
    StorageNativeMigration,
    StorageNativeMigrationStore,
    StorageNativeSandboxManifest,
)
from ucloud_sandboxes.hibernation import (  # noqa: E402
    HibernationRuntimeFingerprint,
)
from ucloud_sandboxes.image_rootfs import (  # noqa: E402
    DockerImageConfig,
    MaterializedRootfs,
    OverlayRootfsManager,
)
from ucloud_sandboxes.managed_registry import RegistryClient  # noqa: E402
from ucloud_sandboxes.runtime_identity import NodeRuntimeIdentity  # noqa: E402
from ucloud_sandboxes.sandbox import (  # noqa: E402
    SandboxSpec,
    sandbox_spec_fingerprint,
)
from ucloud_sandboxes.storage_native import AgentEnvUblkClient  # noqa: E402
from ucloud_sandboxes.storage_native_daemon import (  # noqa: E402
    StorageNativeNodeClient,
    StorageNativeNodeConfig,
    StorageNativeNodeServer,
    StorageNativeNodeService,
)
from ucloud_sandboxes.storage_native_registry import (  # noqa: E402
    PublishedStorageLayer,
    RegistrySnapshotPublisher,
    StorageSnapshotPublication,
)


MIB = 1024 * 1024
GIB = 1024 * MIB


class FixedImageStore:
    def __init__(self, images: Path, image: MaterializedRootfs) -> None:
        self.images = images
        self.image = image

    @contextlib.contextmanager
    def operation_lease(self, image_ref: str):
        if image_ref != self.image.image_ref:
            raise RuntimeError("unexpected benchmark image reference")
        yield self.image


def _wait_client(client: StorageNativeNodeClient, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            client.get_features()
            return
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)


def _start_server(
    socket_path: Path,
    service: StorageNativeNodeService,
) -> tuple[StorageNativeNodeServer, threading.Thread]:
    server = StorageNativeNodeServer(socket_path, service)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _wait_client(StorageNativeNodeClient(socket_path))
    return server, thread


def _stop_server(
    server: StorageNativeNodeServer,
    thread: threading.Thread,
) -> None:
    server.shutdown()
    thread.join(timeout=10)
    if thread.is_alive():
        raise RuntimeError("storage-native service did not stop")


def _run(*argv: str, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {argv!r}; "
            f"stdout={result.stdout!r}; stderr={result.stderr!r}"
        )
    return result


def _oci_config(namespace: str) -> dict[str, Any]:
    return {
        "hostname": "storage-native-warden",
        "linux": {
            "namespaces": [
                {"type": "pid"},
                {"path": f"/run/netns/{namespace}", "type": "network"},
                {"type": "ipc"},
                {"type": "uts"},
                {"type": "mount"},
            ],
            "resources": {
                "memory": {
                    "limit": 512 * MIB,
                    "swap": 512 * MIB,
                }
            },
        },
        "mounts": [
            {
                "destination": "/proc",
                "options": ["nosuid", "noexec", "nodev"],
                "source": "proc",
                "type": "proc",
            },
            {
                "destination": "/dev",
                "options": ["nosuid", "mode=755", "size=65536k"],
                "source": "tmpfs",
                "type": "tmpfs",
            },
            {
                "destination": "/dev/pts",
                "options": [
                    "nosuid",
                    "noexec",
                    "newinstance",
                    "ptmxmode=0666",
                    "mode=0620",
                    "gid=5",
                ],
                "source": "devpts",
                "type": "devpts",
            },
            {
                "destination": "/dev/shm",
                "options": [
                    "nosuid",
                    "noexec",
                    "nodev",
                    "mode=1777",
                    "size=65536k",
                ],
                "source": "shm",
                "type": "tmpfs",
            },
            {
                "destination": "/dev/mqueue",
                "options": ["nosuid", "noexec", "nodev"],
                "source": "mqueue",
                "type": "mqueue",
            },
            {
                "destination": "/sys",
                "options": ["nosuid", "noexec", "nodev", "ro"],
                "source": "sysfs",
                "type": "sysfs",
            },
            {
                "destination": "/tmp",
                "options": [
                    "nosuid",
                    "nodev",
                    "mode=1777",
                    "size=67108864",
                ],
                "source": "tmpfs",
                "type": "tmpfs",
            },
        ],
        "ociVersion": "1.0.2",
        "process": {
            "args": ["/conformance-workload", "server"],
            "capabilities": {
                name: []
                for name in (
                    "bounding",
                    "effective",
                    "inheritable",
                    "permitted",
                )
            },
            "cwd": "/",
            "env": ["PATH=/"],
            "noNewPrivileges": True,
            "terminal": False,
            "user": {"gid": 0, "uid": 0},
        },
        "root": {"path": "rootfs", "readonly": False},
    }


def _cpu_features_sha256() -> str:
    lines = Path("/proc/cpuinfo").read_text(encoding="ascii").splitlines()
    features = next(
        line for line in lines if line.startswith(("flags", "Features"))
    )
    return hashlib.sha256(features.encode("ascii")).hexdigest()


def _runtime_fingerprint(runsc: Path) -> HibernationRuntimeFingerprint:
    return HibernationRuntimeFingerprint(
        runsc_sha256=hashlib.sha256(runsc.read_bytes()).hexdigest(),
        runsc_commit="0" * 40,
        platform="systrap",
        architecture=os.uname().machine,
        page_size=os.sysconf("SC_PAGE_SIZE"),
        cpu_features_sha256=_cpu_features_sha256(),
        boot_config_sha256=hashlib.sha256(
            b"storage-native-warden-qualification-v1"
        ).hexdigest(),
        rootfs_sha256="0" * 64,
    )


def _record(client: StorageNativeNodeClient, volume_id: str) -> dict[str, Any]:
    raw = client.get_volume(volume_id).get("record")
    if not isinstance(raw, dict):
        raise RuntimeError("storage service returned an invalid volume record")
    return raw


def _publication(record: dict[str, Any]) -> StorageSnapshotPublication:
    layers = record.get("published_layers")
    if not isinstance(layers, list):
        raise RuntimeError("published storage record has no layer inventory")
    return StorageSnapshotPublication(
        manifest_digest=str(record.get("published_manifest_digest") or ""),
        tag=str(record.get("published_tag") or ""),
        repository=str(record.get("published_repository") or ""),
        repo_blob_url=str(record.get("published_repo_blob_url") or ""),
        virtual_size=int(record.get("virtual_size") or 0),
        layers=tuple(PublishedStorageLayer.from_dict(item) for item in layers),
    )


def _registry_metrics(path: Path | None) -> dict[str, int]:
    if path is None:
        return {}
    try:
        raw = json.loads(path.read_text(encoding="ascii"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        key: int(raw.get(key) or 0)
        for key in ("bytes_served", "requests")
    }


def _metrics_delta(
    before: dict[str, int],
    after: dict[str, int],
) -> dict[str, int]:
    return {
        key: after.get(key, 0) - before.get(key, 0)
        for key in after
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("Warden benchmark requires Linux root")
    for tool in ("fsfreeze", "ip", "mkfs.xfs", "mount", "mountpoint", "umount"):
        if shutil.which(tool) is None:
            raise RuntimeError(f"required host tool is missing: {tool}")
    for path in (
        args.daemon,
        args.runsc,
        args.conformance_workload,
        args.noop_workload,
    ):
        if not path.is_file() or not os.access(path, os.X_OK):
            raise RuntimeError(f"required executable is unavailable: {path}")

    root = Path(
        tempfile.mkdtemp(
            prefix="ucloud-storage-native-warden-",
            dir=args.work_root,
        )
    ).resolve()
    backend_process: subprocess.Popen[str] | None = None
    backend_client: AgentEnvUblkClient | None = None
    server: StorageNativeNodeServer | None = None
    thread: threading.Thread | None = None
    namespace = f"ucloud-storage-warden-{os.getpid()}"
    namespace_created = False
    overlay: OverlayRootfsManager | None = None
    sandbox = None
    client: StorageNativeNodeClient | None = None
    result: dict[str, Any] = {
        "schema": 1,
        "status": "failed",
        "test_root": str(root),
    }
    registry_metrics_before = _registry_metrics(args.registry_metrics)
    try:
        cache = root / "cache"
        cache.mkdir()
        global_config = root / "global.json"
        global_config.write_text(
            json.dumps(
                {
                    "cacheConfig": {
                        "cacheDir": str(cache),
                        "cacheSizeGB": 1,
                        "cacheType": "file",
                        "refillSize": 262144,
                    },
                    "download": {"enable": False},
                    "nrIoRings": 1,
                    "registryFsVersion": "v2",
                },
                sort_keys=True,
            )
            + "\n",
            encoding="ascii",
        )
        backend_socket = root / "backend.sock"
        backend_log = (root / "backend.log").open("w", encoding="utf-8")
        backend_process = subprocess.Popen(
            [
                str(args.daemon),
                "--socket-path",
                str(backend_socket),
                "--global-config",
                str(global_config),
                "--metrics-listen-addr",
                "",
            ],
            stdin=subprocess.DEVNULL,
            stdout=backend_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        backend_log.close()
        backend_client = AgentEnvUblkClient(backend_socket)
        backend_client.wait_ready()

        mount_root = root / "volumes"
        config = StorageNativeNodeConfig(
            journal_path=root / "storage-journal" / "storage.sqlite",
            runtime_root=root / "storage-runtime",
            mount_root=mount_root,
            hard_capacity_bytes=4 * GIB,
        )
        publisher = (
            RegistrySnapshotPublisher(
                RegistryClient(args.registry_url, timeout_seconds=120),
                repository=args.repository,
                stream_socket_root=Path(
                    "/run/ucloud-storage-native-publication"
                ),
            )
            if args.registry_url
            else None
        )
        service = StorageNativeNodeService(
            config,
            backend=backend_client,
            global_config_path=global_config,
            publisher=publisher,
        )
        service_socket = root / "service" / "storage.sock"
        server, thread = _start_server(service_socket, service)
        client = StorageNativeNodeClient(service_socket)

        sandbox_id = "warden-benchmark"
        sandbox_generation = 1
        volume_id = f"{sandbox_id}.sandbox-{sandbox_generation}"
        started = time.monotonic()
        client.create_volume(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            volume_id=volume_id,
            operation_id="create-volume:1",
            virtual_size=GIB,
            accounting_id=1,
        )
        create_volume_seconds = time.monotonic() - started

        images = root / "images"
        lower = images / ("a" * 64) / "rootfs"
        lower.mkdir(mode=0o755, parents=True)
        for directory in ("dev", "proc", "run", "sys", "tmp"):
            (lower / directory).mkdir(mode=0o755)
        for source, name in (
            (args.conformance_workload, "conformance-workload"),
            (args.noop_workload, "noop"),
        ):
            shutil.copyfile(source, lower / name)
            (lower / name).chmod(0o755)
        rootfs_identity = hashlib.sha256(
            b"storage-native-warden-test-rootfs"
        ).hexdigest()
        image = MaterializedRootfs(
            image_ref="storage-native:test",
            image_id="sha256:" + "a" * 64,
            rootfs_identity_sha256=rootfs_identity,
            rootfs=lower,
            image_config=DockerImageConfig(),
        )
        image_store = FixedImageStore(images, image)
        overlay = OverlayRootfsManager(
            image_store,  # type: ignore[arg-type]
            writable_root=mount_root,
            bundle_root=root / "bundles",
            require_precreated_writable=True,
        )

        _run("ip", "netns", "add", namespace)
        namespace_created = True
        _run("ip", "netns", "exec", namespace, "ip", "link", "set", "lo", "up")
        spec = SandboxSpec(
            id=sandbox_id,
            image=image.image_ref,
            memory_mb=512,
            cpus=1.0,
            disk_mb=1024,
            network="none",
            parkable=True,
        )
        spec.validate()
        spec_sha256 = sandbox_spec_fingerprint(spec)
        lease = overlay.prepare(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            image=image,
            config_template=_oci_config(namespace),
            spec_sha256=spec_sha256,
        )
        sandbox = lease.sandbox
        warden = DirectRunscWarden(
            DirectRunscWardenConfig(
                runsc=args.runsc,
                runtime_root=root / "runsc",
                memory_root=mount_root,
                bundle_root=root / "bundles",
                journal_root=root / "warden-journal",
                artifact_root=mount_root,
                runtime_fingerprint=_runtime_fingerprint(args.runsc),
                network="sandbox",
                command_timeout_seconds=120,
                restore_background=True,
                restore_cpu_startup_burst=False,
                restore_reflink=False,
                restore_start_paused=True,
                allow_connected_on_save=True,
                readiness_command=("/noop",),
                remove_memory_directory_on_delete=False,
            ),
            storage=client,
            rootfs_lifecycle=overlay,
        )

        started = time.monotonic()
        warden.create(sandbox, operation_id="create-runtime:1")
        create_runtime_seconds = time.monotonic() - started
        initial = warden.exec(
            sandbox,
            ("/conformance-workload", "client"),
        ).stdout.strip()
        if not initial.startswith("ok "):
            raise RuntimeError(f"initial conformance failed: {initial!r}")

        started = time.monotonic()
        parked = warden.park(sandbox, operation_id="park:1")
        park_seconds = time.monotonic() - started
        parked_storage = _record(client, volume_id)
        if parked.state.value != "parked":
            raise RuntimeError("Warden did not commit PARKED")
        if parked_storage["state"] != "released":
            raise RuntimeError("parked volume retained runtime resources")
        if Path(parked_storage["mount_path"]).is_mount():
            raise RuntimeError("parked volume remained mounted")
        if (sandbox.bundle / "rootfs").is_mount():
            raise RuntimeError("parked overlay remained mounted")

        migration_timings: dict[str, float] = {}
        source_deleted_before_destination_resume = False
        if publisher is not None:
            source_manifest = warden.load_parked_manifest(sandbox)
            portable = StorageNativeSandboxManifest(
                spec=spec,
                sandbox_generation=sandbox_generation,
                create_operation_id="create-runtime:1",
                runtime_identity=NodeRuntimeIdentity.from_fingerprint(
                    source_manifest.runtime
                ),
                hibernation_generation=source_manifest.hibernation_generation,
                park_operation_id=source_manifest.operation_id,
                captured_ns=source_manifest.created_ns,
                runtime=source_manifest.runtime,
                source_manifest_sha256=source_manifest.metadata_sha256,
                source_guest_ip=None,
                connection_policy=MIGRATION_CONNECTION_POLICY_NONE,
                files=tuple(
                    StorageNativeArtifactFile(
                        name=item.name,
                        role=item.role,
                        logical_bytes=item.logical_bytes,
                        allocated_bytes=item.allocated_bytes,
                    )
                    for item in source_manifest.files
                ),
            )
            started = time.monotonic()
            source_published_record = warden.publish_storage_snapshot(
                sandbox,
                operation_id="migration:1:publish-source",
            )
            migration_timings["source_publication_seconds"] = (
                time.monotonic() - started
            )
            source_published_layers = source_published_record.get(
                "published_layers"
            )
            if (
                not isinstance(source_published_layers, list)
                or not source_published_layers
            ):
                raise RuntimeError("source publication returned no dense layer")
            migration_timings["source_dense_layer_bytes"] = int(
                source_published_layers[-1]["size"]
            )
            migration = StorageNativeMigration(
                manifest=portable,
                publication=_publication(source_published_record),
            )
            client.delete_volume(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                volume_id=volume_id,
                operation_id="migration:1:delete-source-volume",
                expected_revision=int(source_published_record["revision"]),
            )
            source_deleted_before_destination_resume = True

            _stop_server(server, thread)
            server = None
            thread = None
            destination_mount_root = root / "destination-volumes"
            destination_config = StorageNativeNodeConfig(
                journal_path=(
                    root / "destination-storage-journal" / "storage.sqlite"
                ),
                runtime_root=root / "destination-storage-runtime",
                mount_root=destination_mount_root,
                hard_capacity_bytes=4 * GIB,
            )
            destination_service = StorageNativeNodeService(
                destination_config,
                backend=backend_client,
                global_config_path=global_config,
                publisher=publisher,
            )
            destination_socket = root / "destination-service" / "storage.sock"
            server, thread = _start_server(
                destination_socket,
                destination_service,
            )
            client = StorageNativeNodeClient(destination_socket)

            started = time.monotonic()
            client.acquire_snapshot(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                volume_id=volume_id,
                operation_id="migration:1:acquire-manifest",
                publication=migration.publication.to_dict(),
                accounting_id=1,
            )
            acquired = client.mount_snapshot_cow(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                volume_id=volume_id,
                operation_id="migration:1:mount-destination",
                expected_revision=1,
            )
            migration_timings["destination_acquire_mount_seconds"] = (
                time.monotonic() - started
            )
            migration_timings["destination_acquire_range_delta"] = (
                _metrics_delta(
                    registry_metrics_before,
                    _registry_metrics(args.registry_metrics),
                )
            )
            if acquired["record"]["state"] != "mounted":
                raise RuntimeError("destination snapshot was not mounted")

            destination_overlay = OverlayRootfsManager(
                image_store,  # type: ignore[arg-type]
                writable_root=destination_mount_root,
                bundle_root=root / "destination-bundles",
                require_precreated_writable=True,
            )
            started = time.monotonic()
            destination_lease = destination_overlay.prepare(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                image=image,
                config_template=_oci_config(namespace),
                spec_sha256=spec_sha256,
                imported_parked=True,
            )
            destination_sandbox = destination_lease.sandbox
            destination_warden = DirectRunscWarden(
                DirectRunscWardenConfig(
                    runsc=args.runsc,
                    runtime_root=root / "destination-runsc",
                    memory_root=destination_mount_root,
                    bundle_root=root / "destination-bundles",
                    journal_root=root / "destination-warden-journal",
                    artifact_root=destination_mount_root,
                    runtime_fingerprint=_runtime_fingerprint(args.runsc),
                    network="sandbox",
                    command_timeout_seconds=120,
                    restore_background=True,
                    restore_cpu_startup_burst=False,
                    restore_reflink=False,
                    restore_start_paused=True,
                    allow_connected_on_save=True,
                    readiness_command=("/noop",),
                    remove_memory_directory_on_delete=False,
                ),
                storage=client,
                rootfs_lifecycle=destination_overlay,
            )
            local_manifest = StorageNativeMigrationStore(
                root / "destination-migrations"
            ).rebind_mounted_snapshot(
                migration,
                expected_runtime_identity=portable.runtime_identity,
                expected_runtime=portable.runtime,
                artifact_store=destination_warden.artifacts,
                writable_incarnation=destination_mount_root / volume_id,
            )
            destination_warden.adopt_parked(
                destination_sandbox,
                local_manifest,
            )
            migration_timings["destination_rebind_adopt_seconds"] = (
                time.monotonic() - started
            )
            migration_timings["destination_rebind_range_delta"] = (
                _metrics_delta(
                    registry_metrics_before,
                    _registry_metrics(args.registry_metrics),
                )
            )
            started = time.monotonic()
            destination_published_record = (
                destination_warden.publish_storage_snapshot(
                    destination_sandbox,
                    operation_id="migration:1:publish-destination",
                )
            )
            migration_timings["destination_publication_seconds"] = (
                time.monotonic() - started
            )
            destination_published_layers = destination_published_record.get(
                "published_layers"
            )
            if (
                not isinstance(destination_published_layers, list)
                or len(destination_published_layers) < 2
            ):
                raise RuntimeError(
                    "destination publication did not preserve and extend its chain"
                )
            migration_timings["destination_dense_delta_bytes"] = int(
                destination_published_layers[-1]["size"]
            )
            migration_timings["destination_sealed_file_bytes"] = int(
                destination_published_record["sealed_layer_bytes"]
            )
            migration_timings["destination_layer_count"] = len(
                destination_published_layers
            )
            overlay = destination_overlay
            sandbox = destination_sandbox
            warden = destination_warden

        resume_timings: dict[str, float] = {}
        started = time.monotonic()
        warden.resume(
            sandbox,
            operation_id="resume:1",
            timings=resume_timings,
        )
        resume_seconds = time.monotonic() - started
        metrics_after_resume = _registry_metrics(args.registry_metrics)
        resumed_storage = _record(client, volume_id)
        if resumed_storage["state"] != "mounted":
            raise RuntimeError("resumed volume is not mounted")
        if not (sandbox.bundle / "rootfs").is_mount():
            raise RuntimeError("resumed overlay is not mounted")
        started = time.monotonic()
        warden.exec(sandbox, ("/noop",))
        noop_seconds = time.monotonic() - started
        metrics_after_noop = _registry_metrics(args.registry_metrics)
        response = warden.exec(
            sandbox,
            ("/conformance-workload", "client"),
        ).stdout.strip()
        if not response.startswith("ok "):
            raise RuntimeError(f"restored conformance failed: {response!r}")
        registry_metrics_after = _registry_metrics(args.registry_metrics)
        registry_range_delta = _metrics_delta(
            registry_metrics_before,
            registry_metrics_after,
        )
        registry_phase_deltas = {
            "through_restore_readiness": _metrics_delta(
                registry_metrics_before,
                metrics_after_resume,
            ),
            "explicit_noop": _metrics_delta(
                metrics_after_resume,
                metrics_after_noop,
            ),
            "full_conformance": _metrics_delta(
                metrics_after_noop,
                registry_metrics_after,
            ),
        }

        warden.delete(sandbox)
        overlay.release_sandbox(sandbox)
        overlay = None
        record = _record(client, volume_id)
        client.delete_volume(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            volume_id=volume_id,
            operation_id=f"delete-volume:{record['revision']}",
            expected_revision=int(record["revision"]),
        )
        result.update(
            {
                "create_runtime_seconds": create_runtime_seconds,
                "create_volume_seconds": create_volume_seconds,
                "noop_seconds": noop_seconds,
                "park_seconds": park_seconds,
                "parked_storage": parked_storage,
                "migration_timings": migration_timings,
                "response": response,
                "registry_range_delta": registry_range_delta,
                "registry_phase_deltas": registry_phase_deltas,
                "resume_seconds": resume_seconds,
                "resume_timings_ms": resume_timings,
                "resumed_storage_revision": resumed_storage["revision"],
                "sealed_layer_bytes": parked_storage["sealed_layer_bytes"],
                "source_deleted_before_destination_resume": (
                    source_deleted_before_destination_resume
                ),
                "status": "passed",
                "upper_mode": config.upper_mode,
                "virtual_size": GIB,
            }
        )
        return result
    finally:
        if sandbox is not None and overlay is not None:
            with contextlib.suppress(Exception):
                overlay.release_sandbox(sandbox)
        if namespace_created:
            with contextlib.suppress(Exception):
                _run("ip", "netns", "delete", namespace)
        if server is not None and thread is not None:
            with contextlib.suppress(Exception):
                _stop_server(server, thread)
        if backend_client is not None:
            with contextlib.suppress(Exception):
                backend_client.shutdown()
        if backend_process is not None:
            try:
                backend_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                backend_process.terminate()
                try:
                    backend_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    backend_process.kill()
                    backend_process.wait(timeout=10)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise the production Warden through real ublk/XFS"
    )
    parser.add_argument("--daemon", required=True, type=Path)
    parser.add_argument("--runsc", required=True, type=Path)
    parser.add_argument("--conformance-workload", required=True, type=Path)
    parser.add_argument("--noop-workload", required=True, type=Path)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--registry-url")
    parser.add_argument("--registry-metrics", type=Path)
    parser.add_argument("--repository", default="snapshots")
    args = parser.parse_args()
    for name in (
        "daemon",
        "runsc",
        "conformance_workload",
        "noop_workload",
        "work_root",
        "output",
    ):
        setattr(args, name, getattr(args, name).resolve())
    if args.registry_metrics is not None:
        args.registry_metrics = args.registry_metrics.resolve()
    return args


if __name__ == "__main__":
    arguments = parse_args()
    try:
        payload = run(arguments)
    except BaseException as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        raise
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))
