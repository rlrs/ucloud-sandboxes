#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import contextlib
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

from ucloud_sandboxes.storage_native import AgentEnvUblkClient  # noqa: E402
from ucloud_sandboxes.managed_registry import RegistryClient  # noqa: E402
from ucloud_sandboxes.storage_native_daemon import (  # noqa: E402
    StorageNativeNodeClient,
    StorageNativeNodeConfig,
    StorageNativeNodeServer,
    StorageNativeNodeService,
)
from ucloud_sandboxes.storage_native_registry import (  # noqa: E402
    RegistrySnapshotPublisher,
)


GIB = 1024 * 1024 * 1024


def _latency_summary(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "max_seconds": 0.0, "p50_seconds": 0.0, "p95_seconds": 0.0}

    def percentile(fraction: float) -> float:
        index = min(len(ordered) - 1, int((len(ordered) - 1) * fraction))
        return ordered[index]

    return {
        "count": len(ordered),
        "max_seconds": ordered[-1],
        "p50_seconds": percentile(0.50),
        "p95_seconds": percentile(0.95),
    }


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


def run(args: argparse.Namespace) -> dict[str, Any]:
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("node-service benchmark requires Linux root")
    for tool in ("fsfreeze", "mkfs.xfs", "mount", "umount"):
        if shutil.which(tool) is None:
            raise RuntimeError(f"required host tool is missing: {tool}")
    root = Path(
        tempfile.mkdtemp(
            prefix="ucloud-storage-native-service-",
            dir=args.work_root,
        )
    ).resolve()
    backend_process: subprocess.Popen[str] | None = None
    server: StorageNativeNodeServer | None = None
    thread: threading.Thread | None = None
    backend_client: AgentEnvUblkClient | None = None
    result: dict[str, Any] = {
        "schema": 1,
        "status": "failed",
        "test_root": str(root),
    }
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
            ]
            + (
                [
                    "--enable-pool",
                    "--pool-low-watermark",
                    str(args.pool_low_watermark),
                    "--pool-high-watermark",
                    str(args.pool_high_watermark),
                    "--pool-startup-prewarm",
                    "true",
                ]
                if args.enable_pool
                else []
            ),
            stdin=subprocess.DEVNULL,
            stdout=backend_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        backend_log.close()
        backend_client = AgentEnvUblkClient(backend_socket)
        backend_client.wait_ready()
        config = StorageNativeNodeConfig(
            journal_path=root / "journal" / "storage.sqlite",
            runtime_root=root / "volumes",
            mount_root=root / "mounts",
            hard_capacity_bytes=max(4, args.parallel_volumes + 2) * GIB,
            device_pool_enabled=args.enable_pool,
            device_pool_low_watermark=args.pool_low_watermark,
            device_pool_high_watermark=args.pool_high_watermark,
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

        started = time.monotonic()
        created = client.create_volume(
            sandbox_id="benchmark",
            sandbox_generation=1,
            volume_id="benchmark-1",
            operation_id="create:1",
            virtual_size=GIB,
        )
        create_seconds = time.monotonic() - started
        mount_path = Path(created["record"]["mount_path"])
        payload = mount_path / "payload.bin"
        with payload.open("w+b") as handle:
            os.posix_fallocate(handle.fileno(), 0, 256 * 1024 * 1024)
            handle.seek(0)
            handle.write(b"storage-native-start")
            handle.seek(128 * 1024 * 1024)
            handle.write(b"storage-native-middle")
            handle.flush()
            os.fsync(handle.fileno())

        _stop_server(server, thread)
        server = None
        thread = None
        restarted_service = StorageNativeNodeService(
            config,
            backend=backend_client,
            global_config_path=global_config,
            publisher=publisher,
        )
        reconciliation = restarted_service.reconcile()
        if reconciliation["terminal_records"]:
            raise RuntimeError("stable mounted volume became terminal after restart")
        server, thread = _start_server(service_socket, restarted_service)
        client = StorageNativeNodeClient(service_socket)

        started = time.monotonic()
        sealed = client.freeze_and_seal(
            sandbox_id="benchmark",
            sandbox_generation=1,
            volume_id="benchmark-1",
            operation_id="seal:1",
            expected_revision=1,
        )
        seal_seconds = time.monotonic() - started
        sealed_path = Path(sealed["record"]["sealed_layer_path"])
        sealed_stat = sealed_path.stat()
        started = time.monotonic()
        released = client.release_runtime(
            sandbox_id="benchmark",
            sandbox_generation=1,
            volume_id="benchmark-1",
            operation_id="release:1",
            expected_revision=2,
        )
        release_seconds = time.monotonic() - started
        if released["record"]["state"] != "released":
            raise RuntimeError("volume did not reach released")
        if Path(released["record"]["mount_path"]).is_mount():
            raise RuntimeError("released volume remained mounted")

        publication_seconds = 0.0
        acquire_revision = 3
        destination_service: StorageNativeNodeService | None = None
        if publisher is not None:
            started = time.monotonic()
            published = client.publish_snapshot(
                sandbox_id="benchmark",
                sandbox_generation=1,
                volume_id="benchmark-1",
                operation_id="publish:1",
                expected_revision=3,
            )
            publication_seconds = time.monotonic() - started
            if published["record"]["state"] != "published":
                raise RuntimeError("volume did not become durably published")
            if sealed_path.exists():
                raise RuntimeError("published local layer was not reclaimed")
            publication = published.get("publication")
            if not isinstance(publication, dict):
                raise RuntimeError("publication result is missing its manifest")
            client.delete_volume(
                sandbox_id="benchmark",
                sandbox_generation=1,
                volume_id="benchmark-1",
                operation_id="delete-source:1",
                expected_revision=4,
            )
            destination_config = StorageNativeNodeConfig(
                journal_path=root / "destination-journal" / "storage.sqlite",
                runtime_root=root / "destination-volumes",
                mount_root=root / "destination-mounts",
                hard_capacity_bytes=max(4, args.parallel_volumes + 2) * GIB,
                device_pool_enabled=args.enable_pool,
                device_pool_low_watermark=args.pool_low_watermark,
                device_pool_high_watermark=args.pool_high_watermark,
            )
            destination_service = StorageNativeNodeService(
                destination_config,
                backend=backend_client,
                global_config_path=global_config,
                publisher=publisher,
            )
            acquired = destination_service.acquire_snapshot(
                sandbox_id="benchmark",
                sandbox_generation=1,
                volume_id="benchmark-1",
                operation_id="import-destination:1",
                publication_raw=publication,
            )
            if acquired["record"]["state"] != "published":
                raise RuntimeError("destination did not acquire parked authority")

        started = time.monotonic()
        if destination_service is not None:
            resumed = destination_service.mount_snapshot_cow(
                sandbox_id="benchmark",
                sandbox_generation=1,
                volume_id="benchmark-1",
                operation_id="acquire:destination",
                expected_revision=1,
            )
        else:
            resumed = client.mount_snapshot_cow(
                sandbox_id="benchmark",
                sandbox_generation=1,
                volume_id="benchmark-1",
                operation_id="acquire:1",
                expected_revision=acquire_revision,
            )
        acquire_seconds = time.monotonic() - started
        resumed_payload = Path(resumed["record"]["mount_path"]) / "payload.bin"
        with resumed_payload.open("rb") as handle:
            if handle.read(len(b"storage-native-start")) != b"storage-native-start":
                raise RuntimeError("snapshot start sentinel did not survive")
            handle.seek(128 * 1024 * 1024)
            if handle.read(len(b"storage-native-middle")) != b"storage-native-middle":
                raise RuntimeError("snapshot middle sentinel did not survive")
        active_storage: Any = destination_service or client
        second_seal = active_storage.freeze_and_seal(
            sandbox_id="benchmark",
            sandbox_generation=1,
            volume_id="benchmark-1",
            operation_id="seal:2",
            expected_revision=(
                2 if destination_service is not None else acquire_revision + 1
            ),
        )
        expected_local_layers = 1 if publisher is not None else 2
        if (
            len(second_seal["record"]["sealed_layer_paths"])
            != expected_local_layers
        ):
            raise RuntimeError("resumed snapshot did not preserve its lower chain")
        if publisher is not None and len(
            second_seal["record"]["published_layers"]
        ) != 1:
            raise RuntimeError("resumed snapshot lost its published lower")
        final_release = active_storage.release_runtime(
            sandbox_id="benchmark",
            sandbox_generation=1,
            volume_id="benchmark-1",
            operation_id="release:2",
            expected_revision=(
                3 if destination_service is not None else acquire_revision + 2
            ),
        )

        churn_acquire_seconds: list[float] = []
        churn_release_seconds: list[float] = []
        current_revision = int(final_release["record"]["revision"])
        for index in range(args.churn_iterations):
            started = time.monotonic()
            churn_mounted = active_storage.mount_snapshot_cow(
                sandbox_id="benchmark",
                sandbox_generation=1,
                volume_id="benchmark-1",
                operation_id=f"churn-acquire:{index}",
                expected_revision=current_revision,
            )
            churn_acquire_seconds.append(time.monotonic() - started)
            started = time.monotonic()
            churn_released = active_storage.discard_mounted_cow(
                sandbox_id="benchmark",
                sandbox_generation=1,
                volume_id="benchmark-1",
                operation_id=f"churn-release:{index}",
                expected_revision=int(churn_mounted["record"]["revision"]),
            )
            churn_release_seconds.append(time.monotonic() - started)
            current_revision = int(churn_released["record"]["revision"])

        parallel_revisions: dict[int, int] = {}
        parallel_acquire_seconds: list[float] = []
        parallel_release_seconds: list[float] = []

        def seed_parallel_volume(index: int) -> tuple[int, int]:
            sandbox_id = f"parallel-{index}"
            volume_id = f"parallel-{index}"
            created_parallel = active_storage.create_volume(
                sandbox_id=sandbox_id,
                sandbox_generation=1,
                volume_id=volume_id,
                operation_id=f"parallel-{index}:create",
                virtual_size=GIB,
            )
            sealed_parallel = active_storage.freeze_and_seal(
                sandbox_id=sandbox_id,
                sandbox_generation=1,
                volume_id=volume_id,
                operation_id=f"parallel-{index}:seal",
                expected_revision=int(created_parallel["record"]["revision"]),
            )
            released_parallel = active_storage.release_runtime(
                sandbox_id=sandbox_id,
                sandbox_generation=1,
                volume_id=volume_id,
                operation_id=f"parallel-{index}:release",
                expected_revision=int(sealed_parallel["record"]["revision"]),
            )
            return index, int(released_parallel["record"]["revision"])

        parallel_seed_started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.parallel_volumes or 1) as pool:
            for index, revision in pool.map(
                seed_parallel_volume,
                range(args.parallel_volumes),
            ):
                parallel_revisions[index] = revision
        parallel_seed_seconds = time.monotonic() - parallel_seed_started

        def churn_parallel_volume(
            item: tuple[int, int],
            round_index: int,
        ) -> tuple[int, int, float, float]:
            index, revision = item
            sandbox_id = f"parallel-{index}"
            volume_id = f"parallel-{index}"
            started = time.monotonic()
            mounted_parallel = active_storage.mount_snapshot_cow(
                sandbox_id=sandbox_id,
                sandbox_generation=1,
                volume_id=volume_id,
                operation_id=f"parallel-{index}:acquire:{round_index}",
                expected_revision=revision,
            )
            acquire_latency = time.monotonic() - started
            started = time.monotonic()
            released_parallel = active_storage.discard_mounted_cow(
                sandbox_id=sandbox_id,
                sandbox_generation=1,
                volume_id=volume_id,
                operation_id=f"parallel-{index}:release:{round_index}",
                expected_revision=int(mounted_parallel["record"]["revision"]),
            )
            release_latency = time.monotonic() - started
            return (
                index,
                int(released_parallel["record"]["revision"]),
                acquire_latency,
                release_latency,
            )

        parallel_churn_started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.parallel_volumes or 1) as pool:
            for round_index in range(args.parallel_rounds):
                futures = [
                    pool.submit(churn_parallel_volume, item, round_index)
                    for item in sorted(parallel_revisions.items())
                ]
                for future in futures:
                    index, revision, acquire_latency, release_latency = future.result()
                    parallel_revisions[index] = revision
                    parallel_acquire_seconds.append(acquire_latency)
                    parallel_release_seconds.append(release_latency)
        parallel_churn_seconds = time.monotonic() - parallel_churn_started

        final_metrics = (
            destination_service.metrics()
            if destination_service is not None
            else client.get_metrics()
        )

        for index, revision in sorted(parallel_revisions.items()):
            active_storage.delete_volume(
                sandbox_id=f"parallel-{index}",
                sandbox_generation=1,
                volume_id=f"parallel-{index}",
                operation_id=f"parallel-{index}:delete",
                expected_revision=revision,
            )
        active_storage.delete_volume(
            sandbox_id="benchmark",
            sandbox_generation=1,
            volume_id="benchmark-1",
            operation_id="delete:benchmark",
            expected_revision=current_revision,
        )
        post_cleanup_metrics = (
            destination_service.metrics()
            if destination_service is not None
            else client.get_metrics()
        )
        if post_cleanup_metrics["hard_reserved_bytes"] != 0:
            raise RuntimeError("benchmark leaked a hard storage reservation")

        result.update(
            {
                "acquire_seconds": acquire_seconds,
                "create_seconds": create_seconds,
                "reconciliation": reconciliation,
                "release_seconds": release_seconds,
                "publication_seconds": publication_seconds,
                "source_deleted_before_destination_resume": (
                    destination_service is not None
                ),
                "seal_seconds": seal_seconds,
                "sealed_allocated_bytes": sealed_stat.st_blocks * 512,
                "sealed_file_bytes": sealed_stat.st_size,
                "status": "passed",
                "device_pool_enabled": args.enable_pool,
                "device_pool_low_watermark": args.pool_low_watermark,
                "device_pool_high_watermark": args.pool_high_watermark,
                "churn_acquire": _latency_summary(churn_acquire_seconds),
                "churn_release": _latency_summary(churn_release_seconds),
                "churn_iterations": args.churn_iterations,
                "final_metrics": final_metrics,
                "parallel_volumes": args.parallel_volumes,
                "parallel_rounds": args.parallel_rounds,
                "parallel_seed_seconds": parallel_seed_seconds,
                "parallel_churn_seconds": parallel_churn_seconds,
                "parallel_acquire": _latency_summary(parallel_acquire_seconds),
                "parallel_release": _latency_summary(parallel_release_seconds),
                "post_cleanup_metrics": post_cleanup_metrics,
                "upper_mode": config.upper_mode,
                "virtual_size": GIB,
            }
        )
        return result
    finally:
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
                backend_process.wait(timeout=10)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise the journaled node service on real ublk/XFS"
    )
    parser.add_argument("--daemon", required=True, type=Path)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--registry-url")
    parser.add_argument("--repository", default="snapshots")
    parser.add_argument("--enable-pool", action="store_true")
    parser.add_argument("--pool-low-watermark", type=int, default=2)
    parser.add_argument("--pool-high-watermark", type=int, default=16)
    parser.add_argument("--churn-iterations", type=int, default=100)
    parser.add_argument("--parallel-volumes", type=int, default=8)
    parser.add_argument("--parallel-rounds", type=int, default=10)
    args = parser.parse_args()
    args.daemon = args.daemon.resolve()
    args.work_root = args.work_root.resolve()
    args.output = args.output.resolve()
    if args.pool_low_watermark < 0:
        parser.error("--pool-low-watermark cannot be negative")
    if args.pool_high_watermark < 1:
        parser.error("--pool-high-watermark must be positive")
    if args.pool_low_watermark > args.pool_high_watermark:
        parser.error("pool low watermark cannot exceed high watermark")
    if args.churn_iterations < 0:
        parser.error("--churn-iterations cannot be negative")
    if args.parallel_volumes < 0:
        parser.error("--parallel-volumes cannot be negative")
    if args.parallel_rounds < 0:
        parser.error("--parallel-rounds cannot be negative")
    return args


def main() -> int:
    args = parse_args()
    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
