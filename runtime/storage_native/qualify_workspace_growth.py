#!/usr/bin/env python3
"""Qualify grow-on-demand workspaces on real ublk/XFS (docs/disk-density.md).

Drives the journaled storage-native service with a private AgentEnv backend:

1. A 4 GiB workspace formatted at a 1 GiB grant exposes a 1 GiB filesystem.
2. Filling it fails with ENOSPC at the grant; a neighbour volume keeps working.
3. Delete/rewrite churn keeps the upper's allocated bytes within the grant,
   the physical bound the node's claim relies on.
4. Online growth under concurrent writes reaches the ceiling, and the data
   written across every growth step verifies.
5. Seal, release and remount preserve the grown size (superblock readback).
6. Growth driven like the node monitor (statvfs poll + step) against a
   streaming writer, recording whether the writer ever saw ENOSPC early.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
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
sys.path.insert(0, str(Path(__file__).resolve().parent))

from benchmark_node_service import _start_server, _stop_server  # noqa: E402
from ucloud_sandboxes.disk_claims import next_grant  # noqa: E402
from ucloud_sandboxes.storage_native import AgentEnvUblkClient  # noqa: E402
from ucloud_sandboxes.storage_native_daemon import (  # noqa: E402
    LinuxStorageHostOperations,
    StorageNativeNodeClient,
    StorageNativeNodeConfig,
    StorageNativeNodeService,
    StorageVolumeOwner,
    StorageVolumeState,
)

MIB = 1024**2
GIB = 1024**3
CHUNK = 8 * MIB


def _allocated(path: Path) -> int:
    seen, total = set(), 0
    for directory, _names, files in os.walk(path):
        for name in files:
            info = os.lstat(os.path.join(directory, name))
            if (info.st_dev, info.st_ino) not in seen:
                seen.add((info.st_dev, info.st_ino))
                total += info.st_blocks * 512
    return total


def _fill(path: Path, limit: int, *, seed: bytes) -> tuple[int, str, bool]:
    """Write pseudo-random chunks until ``limit`` bytes or ENOSPC."""
    digest = hashlib.sha256()
    written, enospc = 0, False
    block = hashlib.sha256(seed).digest() * (CHUNK // 32)
    with path.open("wb") as handle:
        try:
            while written < limit:
                chunk = block[: min(CHUNK, limit - written)]
                handle.write(chunk)
                handle.flush()
                digest.update(chunk)
                written += len(chunk)
            os.fsync(handle.fileno())
        except OSError as exc:
            if exc.errno not in {errno.ENOSPC, errno.EDQUOT}:
                raise
            enospc = True
    return written, digest.hexdigest(), enospc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(CHUNK):
            digest.update(block)
    return digest.hexdigest()


def _global_config(root: Path, name: str) -> Path:
    cache = root / f"{name}-cache"
    cache.mkdir()
    path = root / f"{name}.json"
    path.write_text(json.dumps({
        "cacheConfig": {"cacheDir": str(cache), "cacheSizeGB": 1, "cacheType": "file",
                        "refillSize": 262144},
        "download": {"enable": False}, "nrIoRings": 1, "registryFsVersion": "v2",
    }, sort_keys=True) + "\n", encoding="ascii")
    return path


def run(args: argparse.Namespace) -> dict[str, Any]:
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("workspace growth qualification requires Linux root")
    for tool in ("fsfreeze", "mkfs.xfs", "xfs_growfs", "mount", "umount"):
        if shutil.which(tool) is None:
            raise RuntimeError(f"required host tool is missing: {tool}")
    root = Path(tempfile.mkdtemp(prefix="ucloud-workspace-growth-", dir=args.work_root)).resolve()
    result: dict[str, Any] = {"schema": 1, "status": "failed", "test_root": str(root),
                              "upper_mode": args.upper_mode}
    backend_process = server = thread = backend_client = None
    try:
        global_config = _global_config(root, "global")
        resize_config = _global_config(root, "resize-global")
        backend_socket = root / "backend.sock"
        with (root / "backend.log").open("w", encoding="utf-8") as log:
            backend_process = subprocess.Popen(
                [str(args.daemon), "--socket-path", str(backend_socket), "--global-config",
                 str(global_config), "--resize-global-config", str(resize_config),
                 "--metrics-listen-addr", ""],
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, text=True,
            )
        backend_client = AgentEnvUblkClient(backend_socket)
        backend_client.wait_ready()
        config = StorageNativeNodeConfig(
            journal_path=root / "journal" / "storage.sqlite",
            runtime_root=root / "volumes", mount_root=root / "mounts",
            hard_capacity_bytes=64 * GIB, upper_mode=args.upper_mode,
        )
        service = StorageNativeNodeService(config, backend=backend_client,
                                           global_config_path=global_config)
        socket_path = root / "service" / "storage.sock"
        server, thread = _start_server(socket_path, service)
        client = StorageNativeNodeClient(socket_path)
        host = LinuxStorageHostOperations()
        owner = StorageVolumeOwner("growth-1", "growth", 1)

        # 1. Grant-sized filesystem on a ceiling-sized device.
        started = time.monotonic()
        created = client.prepare_volume(owner, operation_id="create:1", virtual_size=4 * GIB,
                                        granted_size=GIB)
        result["create_seconds"] = time.monotonic() - started
        mount = Path(created.mount_path)
        fs_bytes = host.filesystem_bytes(mount)
        statvfs = os.statvfs(mount)
        result["initial"] = {"granted_size": created.granted_size, "filesystem_bytes": fs_bytes,
                             "statvfs_total": statvfs.f_blocks * statvfs.f_frsize,
                             "statvfs_free": statvfs.f_bavail * statvfs.f_frsize,
                             "metrics_hard_reserved": client.get_metrics()["hard_reserved_bytes"]}
        if fs_bytes != GIB or created.granted_size != GIB:
            raise RuntimeError("workspace filesystem was not formatted at its grant")

        # 2. ENOSPC at the grant, contained to this volume.
        written, _, enospc = _fill(mount / "fill.bin", 2 * GIB, seed=b"fill")
        neighbour_owner = StorageVolumeOwner("growth-2", "neighbour", 1)
        neighbour = client.prepare_volume(neighbour_owner, operation_id="create:2",
                                          virtual_size=GIB)
        neighbour_written, _, neighbour_enospc = _fill(
            Path(neighbour.mount_path) / "ok.bin", 256 * MIB, seed=b"neighbour")
        result["enospc"] = {"written": written, "enospc": enospc,
                            "neighbour_written": neighbour_written,
                            "neighbour_enospc": neighbour_enospc}
        if not enospc or written > GIB or neighbour_enospc:
            raise RuntimeError("ENOSPC was not bounded by the grant or leaked to a neighbour")

        # 3. Churn: the live upper stays within the grant.
        runtime_dir = Path(client.get_volume(owner.volume_id).runtime_dir)
        churn = []
        for round_index in range(args.churn_rounds):
            (mount / "fill.bin").unlink(missing_ok=True)
            os.sync()
            _fill(mount / "fill.bin", 700 * MIB, seed=f"churn-{round_index}".encode())
            os.sync()
            churn.append(_allocated(runtime_dir))
        result["churn_upper_allocated"] = churn
        result["churn_upper_bound_ok"] = max(churn) <= GIB + 64 * MIB
        (mount / "fill.bin").unlink()

        # 4. Online growth under concurrent writes up to the ceiling.
        files: dict[str, str] = {}
        grow_seconds = []
        writer_errors: list[BaseException] = []
        for step, target in enumerate((2 * GIB, 3 * GIB, 4 * GIB)):
            name = f"step-{step}.bin"
            writer_result: dict[str, Any] = {}

            def writer(name=name, writer_result=writer_result):
                try:
                    writer_result["value"] = _fill(mount / name, 700 * MIB, seed=name.encode())
                except BaseException as exc:  # reported below
                    writer_errors.append(exc)

            background = threading.Thread(target=writer)
            background.start()
            time.sleep(0.2)
            started = time.monotonic()
            grown = client.grow_volume(owner, granted_size=target)
            grow_seconds.append(time.monotonic() - started)
            background.join()
            if writer_errors or grown.granted_size != target or host.filesystem_bytes(mount) != target:
                raise RuntimeError(f"online growth to {target} failed: {writer_errors}")
            files[name] = writer_result["value"][1]
        result["grow_seconds"] = grow_seconds
        for name, digest in files.items():
            if _sha256(mount / name) != digest:
                raise RuntimeError(f"{name} changed across growth")

        # 5. Seal, release and remount keep the grown filesystem.
        released = client.ensure_released(owner, operation_id="park:1")
        result["released"] = {"charged_bytes": released.charged_bytes,
                              "local_layer_bytes": released.local_layer_bytes}
        remounted = client.ensure_mounted(owner, operation_id="wake:1")
        result["remount_granted_size"] = remounted.granted_size
        if remounted.granted_size != 4 * GIB or host.filesystem_bytes(mount) != 4 * GIB:
            raise RuntimeError("remount lost the grown filesystem size")
        for name, digest in files.items():
            if _sha256(mount / name) != digest:
                raise RuntimeError(f"{name} changed across seal and remount")

        # 6. Monitor-driven growth against a streaming writer.
        stream_owner = StorageVolumeOwner("growth-3", "stream", 1)
        streamed = client.prepare_volume(stream_owner, operation_id="create:3",
                                         virtual_size=8 * GIB, granted_size=GIB)
        stream_mount = Path(streamed.mount_path)
        stop = threading.Event()
        growths: list[tuple[float, int]] = []
        granted = [streamed.granted_size]
        started = time.monotonic()

        monitor_errors: list[str] = []

        def monitor():
            try:
                poll()
            except BaseException as exc:
                monitor_errors.append(f"{type(exc).__name__}: {exc}")

        def poll():
            while not stop.wait(args.poll_seconds):
                info = os.statvfs(stream_mount)
                target = next_grant(granted=granted[0], free=info.f_bavail * info.f_frsize,
                                    ceiling=8 * GIB)
                if target is not None:
                    granted[0] = client.grow_volume(stream_owner, granted_size=target).granted_size
                    growths.append((time.monotonic() - started, granted[0]))

        poller = threading.Thread(target=monitor, daemon=True)
        poller.start()
        written, _, early = _fill(stream_mount / "stream.bin", 7 * GIB, seed=b"stream")
        elapsed = time.monotonic() - started
        stop.set()
        poller.join(timeout=10)
        result["monitor_stream"] = {"written": written, "early_enospc": early,
                                    "seconds": elapsed, "mb_per_second": written / MIB / elapsed,
                                    "growths": growths, "monitor_errors": monitor_errors}
        result["final_metrics"] = client.get_metrics()
        result["status"] = "passed" if result["churn_upper_bound_ok"] else "upper-exceeded-grant"
        return result
    except BaseException as exc:
        import traceback
        result["error"] = traceback.format_exc()
        raise
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
                backend_process.kill()
                backend_process.wait(timeout=10)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n",
                               encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--daemon", required=True, type=Path)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--upper-mode", default="hybridLogStructured",
                        choices=("sparse", "logStructured", "hybridLogStructured"))
    parser.add_argument("--churn-rounds", type=int, default=4)
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    args = parser.parse_args()
    args.daemon, args.work_root, args.output = (
        args.daemon.resolve(), args.work_root.resolve(), args.output.resolve())
    result = run(args)
    print(json.dumps({key: result[key] for key in ("status", "churn_upper_allocated",
                                                    "grow_seconds", "monitor_stream")
                      if key in result}, indent=2))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
