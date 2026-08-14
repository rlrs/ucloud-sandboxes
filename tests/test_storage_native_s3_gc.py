from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from hypothesis import given, settings, strategies as st

from tests.test_storage_native_s3 import FakeExporter, FakeS3
from ucloud_sandboxes.storage_native_s3 import S3SnapshotPublisher
from ucloud_sandboxes.storage_native_s3_gc import (
    S3SnapshotGcPlan,
    _unreferenced_marker_for_target,
    execute_s3_snapshot_gc,
    plan_s3_snapshot_gc,
)
from ucloud_sandboxes.storage_native_registry import PublishedStorageLayer


class StorageNativeS3GcTests(unittest.TestCase):
    @settings(max_examples=100, deadline=None, derandomize=True)
    @given(
        originally_planned=st.sets(st.integers(min_value=0, max_value=7)),
        currently_planned=st.sets(st.integers(min_value=0, max_value=7)),
        currently_protected=st.sets(st.integers(min_value=0, max_value=7)),
    )
    def test_execute_deletes_only_candidates_confirmed_by_fresh_plan(
        self,
        originally_planned: set[int],
        currently_planned: set[int],
        currently_protected: set[int],
    ) -> None:
        def key(index: int) -> str:
            return f"p/managed-layers/sha256:{index}"
        original_keys = {key(index) for index in originally_planned}
        current_keys = {key(index) for index in currently_planned}
        protected_keys = {key(index) for index in currently_protected}
        all_keys = original_keys | current_keys | protected_keys
        s3 = FakeS3()
        for object_key in all_keys:
            s3.objects[object_key] = b"data"
            s3.modified_at[object_key] = 0
        original = S3SnapshotGcPlan(
            protected=(),
            candidates=tuple(sorted(original_keys)),
            candidate_bytes=0,
            inventory_objects=len(all_keys),
        )
        current = S3SnapshotGcPlan(
            protected=tuple(sorted(protected_keys)),
            candidates=tuple(sorted(current_keys)),
            candidate_bytes=0,
            inventory_objects=len(all_keys),
        )
        expected_deleted = original_keys & current_keys - protected_keys

        deleted = execute_s3_snapshot_gc(
            s3,
            original,
            max_delete_objects=8,
            revalidate=lambda: current,
        )

        self.assertEqual(deleted, len(expected_deleted))
        self.assertEqual(
            {object_key for object_key in all_keys if object_key not in s3.objects},
            expected_deleted,
        )

    def test_mark_and_sweep_grace_starts_when_reference_disappears(self) -> None:
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

            old_orphan = "production/managed-layers/sha256:orphan-old"
            new_orphan = "production/managed-layers/sha256:orphan-new"
            self.assertNotIn(old_orphan, plan.candidates)
            self.assertIn(old_orphan, plan.markers_to_create)
            self.assertIn(new_orphan, plan.markers_to_create)
            self.assertIn("production/.uploads/abandoned", plan.candidates)
            live_layer = f"production/managed-layers/{publication.layers[0].digest}"
            self.assertIn(live_layer, plan.protected)
            self.assertNotIn(live_layer, plan.candidates)
            self.assertEqual(
                execute_s3_snapshot_gc(
                    s3,
                    plan,
                    max_delete_objects=10,
                    revalidate=lambda: plan,
                ),
                1,
            )
            self.assertIn(live_layer, s3.objects)
            self.assertIn(old_orphan, s3.objects)

            for target in (old_orphan, new_orphan):
                marker = _unreferenced_marker_for_target(target)
                assert marker is not None
                self.assertIn(marker, s3.objects)
                s3.modified_at[marker] = now
            old_marker = _unreferenced_marker_for_target(old_orphan)
            assert old_marker is not None
            s3.modified_at[old_marker] = now - 8 * 24 * 60 * 60

            expired = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=(publication,),
                now=now,
                grace_seconds=7 * 24 * 60 * 60,
            )

            self.assertIn(old_orphan, expired.candidates)
            self.assertNotIn(new_orphan, expired.candidates)
            self.assertEqual(
                execute_s3_snapshot_gc(
                    s3,
                    expired,
                    max_delete_objects=10,
                    revalidate=lambda: expired,
                ),
                1,
            )
            self.assertNotIn(old_orphan, s3.objects)
            self.assertNotIn(old_marker, s3.objects)

    def test_execute_revalidates_reference_that_returned_after_plan(self) -> None:
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
            target = "production/managed-layers/sha256:returning"
            s3.objects[target] = b"returning"
            now = 1_000_000.0
            s3.modified_at[target] = now - 30 * 24 * 60 * 60
            first = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=(publication,),
                now=now,
                grace_seconds=7 * 24 * 60 * 60,
            )
            execute_s3_snapshot_gc(
                s3,
                first,
                max_delete_objects=10,
                revalidate=lambda: first,
            )
            marker = _unreferenced_marker_for_target(target)
            assert marker is not None
            s3.modified_at[marker] = now - 8 * 24 * 60 * 60
            stale = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=(publication,),
                now=now,
                grace_seconds=7 * 24 * 60 * 60,
            )
            self.assertIn(target, stale.candidates)
            referenced = replace(
                publication,
                layers=(PublishedStorageLayer("sha256:returning", len(b"returning")),),
            )

            protected = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=(referenced,),
                now=now,
                grace_seconds=7 * 24 * 60 * 60,
            )

            self.assertNotIn(target, protected.candidates)
            self.assertIn(marker, protected.markers_to_clear)
            deleted = execute_s3_snapshot_gc(
                s3,
                stale,
                max_delete_objects=10,
                revalidate=lambda: protected,
            )
            self.assertEqual(deleted, 0)
            self.assertIn(target, s3.objects)
            self.assertNotIn(marker, s3.objects)

    def test_recent_rewrite_restarts_unreferenced_grace(self) -> None:
        with TemporaryDirectory() as _raw_dir:
            s3 = FakeS3()
            target = "production/metadata/sha256:rewritten.json"
            s3.objects[target] = b"old"
            now = 1_000_000.0
            s3.modified_at[target] = now - 30 * 24 * 60 * 60
            first = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=(),
                now=now,
                grace_seconds=7 * 24 * 60 * 60,
            )
            execute_s3_snapshot_gc(
                s3,
                first,
                max_delete_objects=10,
                revalidate=lambda: first,
            )
            marker = _unreferenced_marker_for_target(target)
            assert marker is not None
            s3.modified_at[marker] = now - 8 * 24 * 60 * 60
            s3.modified_at[target] = now

            rewritten = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=(),
                now=now,
                grace_seconds=7 * 24 * 60 * 60,
            )

            self.assertNotIn(target, rewritten.candidates)
            self.assertIn(target, rewritten.markers_to_create)
            self.assertIn(marker, rewritten.markers_to_clear)
            execute_s3_snapshot_gc(
                s3,
                rewritten,
                max_delete_objects=10,
                revalidate=lambda: rewritten,
            )
            self.assertIn(target, s3.objects)
            self.assertIn(marker, s3.objects)

    def test_execute_refuses_unbounded_candidate_set(self) -> None:
        with TemporaryDirectory() as _raw_dir:
            s3 = FakeS3()
            s3.objects["p/managed-layers/one"] = b"1"
            s3.objects["p/managed-layers/two"] = b"2"
            first = plan_s3_snapshot_gc(
                s3,
                prefix="p",
                publications=(),
                now=10,
                grace_seconds=0,
            )
            execute_s3_snapshot_gc(
                s3,
                first,
                max_delete_objects=10,
                revalidate=lambda: first,
            )
            plan = plan_s3_snapshot_gc(
                s3,
                prefix="p",
                publications=(),
                now=10,
                grace_seconds=0,
            )
            with self.assertRaisesRegex(ValueError, "exceeds"):
                execute_s3_snapshot_gc(
                    s3,
                    plan,
                    max_delete_objects=1,
                    revalidate=lambda: plan,
                )


if __name__ == "__main__":
    unittest.main()
