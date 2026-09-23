from dataclasses import replace
from types import SimpleNamespace
import unittest

from tests.test_control_plane import _portable_snapshot, _sandbox_route
from ucloud_sandboxes.checkpoint_components import MemoryBackingRef, WorkspaceCaptureRef
from ucloud_sandboxes.checkpoint_registry import (
    CheckpointReference, MemoryArtifactBlob, MemoryArtifactPublication, OCI_IMAGE, OCI_INDEX,
)
from ucloud_sandboxes.hibernation import HibernationArtifactStore
from ucloud_sandboxes.models import SandboxInventoryEntry
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.routing import is_portable_parked_route, route_with_inventory_snapshot
from ucloud_sandboxes.storage_native_migration import SPLIT_MIGRATION_SCHEMA, SPLIT_RUNTIME_SCHEMA


def split_snapshot():
    legacy = _portable_snapshot("sandbox")
    manifest = replace(
        legacy.manifest, schema=SPLIT_RUNTIME_SCHEMA,
        workspace=WorkspaceCaptureRef("workspace", "a" * 64),
        memory=MemoryBackingRef("memory", 1024 ** 3),
    )
    files = tuple(MemoryArtifactBlob(f.name, "sha256:" + "b" * 64, 1, f.logical_bytes)
                  for f in manifest.files)
    files += tuple(MemoryArtifactBlob(name, "sha256:" + "c" * 64, 1, 1)
                   for name in (HibernationArtifactStore.MANIFEST_NAME, HibernationArtifactStore.COMPLETE_NAME))
    return replace(
        legacy, manifest=manifest, schema=SPLIT_MIGRATION_SCHEMA,
        memory_publication=MemoryArtifactPublication(
            CheckpointReference("snapshots", "memory", "sha256:" + "d" * 64, OCI_IMAGE, 100),
            manifest.source_manifest_sha256, files),
        checkpoint_publication=CheckpointReference("snapshots", "root", "sha256:" + "e" * 64, OCI_INDEX, 200),
    )


class SplitSnapshotRoutingTests(unittest.TestCase):
    def test_heartbeat_and_route_bind_complete_checkpoint_root(self):
        snapshot = split_snapshot()
        service = SimpleNamespace(cached_storage_native_snapshot=lambda *_: snapshot)
        handler = SimpleNamespace(manager=SimpleNamespace(service=service))
        record = SimpleNamespace(spec=snapshot.manifest.spec, generation=1,
            operation_id=snapshot.manifest.create_operation_id,
            spec_hash=snapshot.manifest.spec_sha256, state="parked")
        entry = NodeAgentHandler._sandbox_inventory_entry(handler, record)
        entry = SandboxInventoryEntry.from_dict(entry.to_dict())
        self.assertEqual(entry.storage_schema, SPLIT_MIGRATION_SCHEMA)
        self.assertEqual(entry.snapshot_manifest_digest, snapshot.reference.manifest_digest)
        self.assertNotEqual(entry.snapshot_manifest_digest, snapshot.publication.manifest_digest)
        route = replace(_sandbox_route(sandbox_id="sandbox", state="parked", node_id="node", job_id="job", node_url="http://node"),
            generation=1, create_operation_id=record.operation_id, spec_hash=record.spec_hash)
        accepted = route_with_inventory_snapshot(route, entry)
        self.assertTrue(is_portable_parked_route(accepted))
        for bad in (replace(entry, storage_schema="storage-native-v1"),
                    replace(entry, snapshot_manifest_digest=snapshot.publication.manifest_digest)):
            with self.assertRaises(ValueError):
                route_with_inventory_snapshot(route, bad)

    def test_import_rejects_envelope_schema_mismatch_before_staging(self):
        snapshot = split_snapshot()
        errors = []
        handler = SimpleNamespace(
            _read_json_body=lambda: {"sandbox_id": "sandbox", "migration_id": "move",
                "storage_schema": "storage-native-v1", "storage_snapshot": snapshot.to_dict(),
                "snapshot_sha256": snapshot.sha256},
            _write_exception=errors.append,
            _direct_service=lambda: self.fail("mismatched schema reached import staging"),
        )
        NodeAgentHandler._import_migration(handler)
        self.assertEqual(len(errors), 1)
        self.assertIn("envelope schema", str(errors[0]))
