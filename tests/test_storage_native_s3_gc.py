from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.test_storage_native_s3 import FakeExporter, FakeS3
from ucloud_sandboxes.storage_native_s3 import S3SnapshotPublisher
from ucloud_sandboxes.storage_native_s3_gc import (
    execute_s3_snapshot_gc,
    plan_s3_snapshot_gc,
)


class StorageNativeS3GcTests(unittest.TestCase):
    def test_mark_and_sweep_retains_live_graph_and_respects_grace(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "delta.commit").resolve()
            source.write_bytes(b"live")
            s3 = FakeS3()
            publisher = S3SnapshotPublisher(
                endpoint="https://fsn1.example",
                bucket="sandbox-bucket",
                region="fsn1",
                prefix="production",
                credential_process="/bin/false",
                stream_socket_root=root,
                upload_chunk_bytes=5 * 1024 * 1024,
                client_factory=lambda: s3,
            )
            publication = publisher.publish(
                exporter=FakeExporter({source: b"live"}),
                source_layer_paths=(source,),
                virtual_size=4096,
            )
            s3.objects["production/managed-layers/sha256:orphan-old"] = b"old"
            s3.objects["production/managed-layers/sha256:orphan-new"] = b"new"
            s3.objects["production/.uploads/abandoned"] = b"partial"
            now = 1_000_000.0
            for key in s3.objects:
                s3.modified_at[key] = now - 10 * 24 * 60 * 60
            s3.modified_at["production/managed-layers/sha256:orphan-new"] = now

            plan = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=(publication,),
                now=now,
                grace_seconds=7 * 24 * 60 * 60,
            )

            self.assertIn(
                "production/managed-layers/sha256:orphan-old", plan.candidates
            )
            self.assertIn("production/.uploads/abandoned", plan.candidates)
            self.assertNotIn(
                "production/managed-layers/sha256:orphan-new", plan.candidates
            )
            live_layer = f"production/managed-layers/{publication.layers[0].digest}"
            self.assertIn(live_layer, plan.protected)
            self.assertNotIn(live_layer, plan.candidates)
            self.assertEqual(
                execute_s3_snapshot_gc(s3, plan, max_delete_objects=10), 2
            )
            self.assertIn(live_layer, s3.objects)

    def test_execute_refuses_unbounded_candidate_set(self) -> None:
        with TemporaryDirectory() as raw_dir:
            s3 = FakeS3()
            s3.objects["p/managed-layers/one"] = b"1"
            s3.objects["p/managed-layers/two"] = b"2"
            plan = plan_s3_snapshot_gc(
                s3,
                prefix="p",
                publications=(),
                now=10,
                grace_seconds=0,
            )
            with self.assertRaisesRegex(ValueError, "exceeds"):
                execute_s3_snapshot_gc(s3, plan, max_delete_objects=1)


if __name__ == "__main__":
    unittest.main()
