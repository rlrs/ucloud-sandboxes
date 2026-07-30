#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from ucloud_sandboxes.direct_migration import DirectMigrationArchiveStore
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.hibernation import (
    HibernationArtifactStore,
    HibernationJournalStore,
    HibernationState,
)
from ucloud_sandboxes.runtime_identity import NodeRuntimeIdentityStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export and optionally re-import one parked direct-runsc sandbox "
            "without changing source ownership."
        )
    )
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--runtime-identity", type=Path, required=True)
    parser.add_argument("--journal-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--sandbox-id", required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument(
        "--destination-root",
        type=Path,
        help="Empty artifact root used to benchmark destination import.",
    )
    return parser.parse_args()


def allocated_bytes(path: Path) -> int:
    total = 0
    for item in path.rglob("*"):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        total += info.st_blocks * 512
    return total


def main() -> int:
    args = parse_args()
    registry = DirectSandboxRegistry(args.registry.resolve())
    registration = registry.get(args.sandbox_id)
    if registration is None or registration.phase != "owned":
        raise SystemExit("sandbox is not an owned direct-runtime registration")
    journal = HibernationJournalStore(args.journal_root.resolve()).journal(
        sandbox_id=registration.sandbox_id,
        sandbox_generation=registration.sandbox_generation,
    )
    lifecycle = journal.load()
    if lifecycle is None or lifecycle.state != HibernationState.PARKED:
        raise SystemExit("sandbox must already be parked")
    source_store = HibernationArtifactStore(args.artifact_root.resolve())
    local_manifest = source_store.load_complete(
        sandbox_id=registration.sandbox_id,
        sandbox_generation=registration.sandbox_generation,
        hibernation_generation=lifecycle.hibernation_generation,
    )
    identity = NodeRuntimeIdentityStore(
        args.runtime_identity.resolve()
    ).load()
    if identity is None:
        raise SystemExit("node runtime identity is absent")
    source = Path(registration.quota_path)
    source_physical = allocated_bytes(source)
    archive_store = DirectMigrationArchiveStore()
    exported = archive_store.export(
        registration=registration,
        local_manifest=local_manifest,
        runtime_identity=identity,
        writable_incarnation=source,
        archive_path=args.archive.resolve(),
    )
    result: dict[str, object] = {
        "sandbox_id": registration.sandbox_id,
        "source_allocated_bytes": source_physical,
        "source_artifact_allocated_bytes": sum(
            item.allocated_bytes for item in local_manifest.files
        ),
        "source_artifact_logical_bytes": sum(
            item.logical_bytes for item in local_manifest.files
        ),
        "archive_path": str(exported.path),
        "archive_sha256": exported.sha256,
        "archive_allocated_bytes": exported.physical_bytes,
        "export_ms": round(exported.elapsed_ms, 3),
        "export_allocated_mib_s": round(
            exported.physical_bytes
            / max(exported.elapsed_ms / 1000, 1e-9)
            / (1024 * 1024),
            3,
        ),
    }
    if args.destination_root is not None:
        destination_root = args.destination_root.resolve()
        destination = destination_root / source.name
        if destination.exists() or any(destination_root.iterdir()):
            raise SystemExit("destination root must exist and be empty")
        started = time.monotonic()
        _, imported = archive_store.import_archive(
            exported.path,
            expected_sha256=exported.sha256,
            expected_runtime_identity=identity,
            expected_runtime=local_manifest.runtime,
            artifact_store=HibernationArtifactStore(destination_root),
            writable_incarnation=destination,
        )
        import_ms = (time.monotonic() - started) * 1000
        result.update(
            {
                "destination_allocated_bytes": allocated_bytes(destination),
                "destination_manifest_sha256": imported.metadata_sha256,
                "import_ms": round(import_ms, 3),
                "import_allocated_mib_s": round(
                    exported.physical_bytes
                    / max(import_ms / 1000, 1e-9)
                    / (1024 * 1024),
                    3,
                ),
            }
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
