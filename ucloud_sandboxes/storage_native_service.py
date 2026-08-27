from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import threading

from .storage_native import AgentEnvUblkClient
from .deployment import package_version
from .managed_registry import RegistryClient
from .storage_native_daemon import (
    StorageNativeNodeConfig,
    StorageNativeNodeServer,
    StorageNativeNodeService,
)
from .storage_native_registry import (
    DEFAULT_COMPACT_AFTER_BYTES,
    DEFAULT_COMPACT_AFTER_LAYERS,
    RegistrySnapshotPublisher,
    SnapshotPublisherRouter,
)
from .storage_native_s3 import S3SnapshotPublisher
from .telemetry import Telemetry, TelemetrySettings


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the privileged UCloud storage-native node service"
    )
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--node-id", required=True)
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
    parser.add_argument("--max-ublk-devices", default=0, type=int)
    parser.add_argument("--snapshot-registry-url")
    parser.add_argument("--snapshot-repository")
    parser.add_argument(
        "--snapshot-backend", choices=("registry", "s3"), default="registry"
    )
    parser.add_argument("--snapshot-s3-endpoint")
    parser.add_argument("--snapshot-s3-bucket")
    parser.add_argument("--snapshot-s3-region")
    parser.add_argument("--snapshot-s3-prefix")
    parser.add_argument("--snapshot-s3-credential-process")
    parser.add_argument("--publication-stream-root", type=Path)
    parser.add_argument("--max-concurrent-publications", default=2, type=int)
    parser.add_argument(
        "--snapshot-compact-after-layers",
        default=DEFAULT_COMPACT_AFTER_LAYERS,
        type=int,
    )
    parser.add_argument(
        "--snapshot-compact-after-bytes",
        default=DEFAULT_COMPACT_AFTER_BYTES,
        type=int,
    )
    parser.add_argument(
        "--publication-upload-chunk-bytes",
        default=8 * 1024 * 1024,
        type=int,
    )
    parser.add_argument(
        "--snapshot-s3-upload-chunk-bytes",
        default=64 * 1024 * 1024,
        type=int,
    )
    parser.add_argument(
        "--snapshot-s3-upload-concurrency",
        default=4,
        type=int,
    )
    parser.add_argument(
        "--upper-mode",
        choices=("sparse", "logStructured", "hybridLogStructured"),
        default="hybridLogStructured",
    )
    parser.add_argument("--telemetry-otlp-endpoint", default="")
    parser.add_argument("--telemetry-cloud-provider", default="")
    parser.add_argument("--telemetry-cloud-machine-type", default="")
    parser.add_argument("--telemetry-trace-sample-ratio", type=float, default=0.1)
    parser.add_argument("--telemetry-export-interval-ms", type=int, default=5_000)
    parser.add_argument("--telemetry-export-timeout-ms", type=int, default=3_000)
    parser.add_argument("--telemetry-max-queue-size", type=int, default=4_096)
    parser.add_argument("--telemetry-max-export-batch-size", type=int, default=512)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("storage-native node service requires Linux root")
    telemetry = Telemetry.create(
        TelemetrySettings(
            endpoint=args.telemetry_otlp_endpoint,
            trace_sample_ratio=args.telemetry_trace_sample_ratio,
            export_interval_ms=args.telemetry_export_interval_ms,
            export_timeout_ms=args.telemetry_export_timeout_ms,
            max_queue_size=args.telemetry_max_queue_size,
            max_export_batch_size=args.telemetry_max_export_batch_size,
        ),
        service_name="ucloud-sandboxes-storage",
        service_version=package_version(),
        deployment_id=args.deployment_id,
        attributes={
            "service.instance.id": args.node_id,
            **(
                {"cloud.provider": args.telemetry_cloud_provider}
                if args.telemetry_cloud_provider
                else {}
            ),
            **(
                {"cloud.machine.type": args.telemetry_cloud_machine_type}
                if args.telemetry_cloud_machine_type
                else {}
            ),
        },
    )
    backend_socket = args.backend_socket.resolve()
    backend = AgentEnvUblkClient(backend_socket)
    backend.wait_ready()
    registry_values = (args.snapshot_registry_url, args.snapshot_repository)
    if any(registry_values) and not all(registry_values):
        raise ValueError(
            "snapshot registry URL and repository must be configured together"
        )
    publisher = None
    registry_publisher = None
    if all(registry_values):
        stream_root = (
            args.publication_stream_root.resolve()
            if args.publication_stream_root is not None
            else Path("/run/ucloud/storage-native-publication")
        )
        registry_publisher = RegistrySnapshotPublisher(
            RegistryClient(args.snapshot_registry_url),
            repository=args.snapshot_repository,
            stream_socket_root=stream_root,
            upload_chunk_bytes=args.publication_upload_chunk_bytes,
            max_concurrent_publications=args.max_concurrent_publications,
            compact_after_layers=args.snapshot_compact_after_layers,
            compact_after_bytes=args.snapshot_compact_after_bytes,
            telemetry=telemetry,
        )
    if args.snapshot_backend == "registry":
        publisher = registry_publisher
    else:
        s3_values = (
            args.snapshot_s3_endpoint,
            args.snapshot_s3_bucket,
            args.snapshot_s3_region,
            args.snapshot_s3_prefix,
            args.snapshot_s3_credential_process,
        )
        if not all(s3_values):
            raise ValueError(
                "S3 snapshot endpoint, bucket, region, prefix, and credential "
                "process must be configured together"
            )
        stream_root = (
            args.publication_stream_root.resolve()
            if args.publication_stream_root is not None
            else Path("/run/ucloud/storage-native-publication")
        )
        s3_publisher = S3SnapshotPublisher(
            endpoint=args.snapshot_s3_endpoint,
            bucket=args.snapshot_s3_bucket,
            region=args.snapshot_s3_region,
            prefix=args.snapshot_s3_prefix,
            credential_process=args.snapshot_s3_credential_process,
            stream_socket_root=stream_root,
            upload_chunk_bytes=args.snapshot_s3_upload_chunk_bytes,
            upload_part_concurrency=args.snapshot_s3_upload_concurrency,
            max_concurrent_publications=args.max_concurrent_publications,
            compact_after_layers=args.snapshot_compact_after_layers,
            compact_after_bytes=args.snapshot_compact_after_bytes,
            telemetry=telemetry,
        )
        verifiers = {"s3": s3_publisher}
        if registry_publisher is not None:
            verifiers["registry"] = registry_publisher
        publisher = SnapshotPublisherRouter(s3_publisher, verifiers=verifiers)
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
            max_ublk_devices=args.max_ublk_devices,
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
    server = StorageNativeNodeServer(
        args.socket.resolve(),
        service,
        telemetry=telemetry,
    )

    def request_shutdown(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    try:
        server.serve_forever()
    finally:
        telemetry.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
