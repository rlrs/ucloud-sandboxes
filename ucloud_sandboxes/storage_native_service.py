from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import threading

from .storage_native import AgentEnvUblkClient
from .managed_registry import RegistryClient
from .storage_native_daemon import (
    StorageNativeNodeConfig,
    StorageNativeNodeServer,
    StorageNativeNodeService,
)
from .storage_native_registry import RegistrySnapshotPublisher


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the privileged UCloud storage-native node service"
    )
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--backend-socket", required=True, type=Path)
    parser.add_argument("--backend-global-config", required=True, type=Path)
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--runtime-root", required=True, type=Path)
    parser.add_argument("--mount-root", required=True, type=Path)
    parser.add_argument("--hard-capacity-bytes", required=True, type=int)
    parser.add_argument("--max-concurrent-operations", default=8, type=int)
    parser.add_argument("--device-pool-enabled", action="store_true")
    parser.add_argument("--device-pool-low-watermark", default=2, type=int)
    parser.add_argument("--device-pool-high-watermark", default=16, type=int)
    parser.add_argument("--snapshot-registry-url")
    parser.add_argument("--snapshot-repository")
    parser.add_argument("--publication-stream-root", type=Path)
    parser.add_argument("--max-concurrent-publications", default=2, type=int)
    parser.add_argument(
        "--publication-upload-chunk-bytes",
        default=8 * 1024 * 1024,
        type=int,
    )
    parser.add_argument(
        "--upper-mode",
        choices=("sparse", "logStructured", "hybridLogStructured"),
        default="hybridLogStructured",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("storage-native node service requires Linux root")
    backend_socket = args.backend_socket.resolve()
    backend = AgentEnvUblkClient(backend_socket)
    backend.wait_ready()
    registry_values = (args.snapshot_registry_url, args.snapshot_repository)
    if any(registry_values) and not all(registry_values):
        raise ValueError(
            "snapshot registry URL and repository must be configured together"
        )
    publisher = None
    if all(registry_values):
        stream_root = (
            args.publication_stream_root.resolve()
            if args.publication_stream_root is not None
            else Path("/run/ucloud/storage-native-publication")
        )
        publisher = RegistrySnapshotPublisher(
            RegistryClient(args.snapshot_registry_url),
            repository=args.snapshot_repository,
            stream_socket_root=stream_root,
            upload_chunk_bytes=args.publication_upload_chunk_bytes,
            max_concurrent_publications=args.max_concurrent_publications,
        )
    service = StorageNativeNodeService(
        StorageNativeNodeConfig(
            journal_path=args.journal.resolve(),
            runtime_root=args.runtime_root.resolve(),
            mount_root=args.mount_root.resolve(),
            hard_capacity_bytes=args.hard_capacity_bytes,
            upper_mode=args.upper_mode,
            max_concurrent_operations=args.max_concurrent_operations,
            device_pool_enabled=args.device_pool_enabled,
            device_pool_low_watermark=args.device_pool_low_watermark,
            device_pool_high_watermark=args.device_pool_high_watermark,
        ),
        backend=backend,
        global_config_path=args.backend_global_config.resolve(),
        publisher=publisher,
    )
    reconciliation = service.reconcile()
    print(
        json.dumps(
            {"event": "storage_native_reconciled", **reconciliation},
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )
    server = StorageNativeNodeServer(args.socket.resolve(), service)

    def request_shutdown(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
