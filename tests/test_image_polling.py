from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import RLock
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from ucloud_sandboxes.control_plane import ControlPlaneHandler
from ucloud_sandboxes.images import ImageRecord, ImageStore
from ucloud_sandboxes.models import utc_now


class ImagePollingTests(unittest.TestCase):
    def handler(self):
        class Handler(ControlPlaneHandler):
            image_build_owners = OrderedDict()
            image_build_owners_lock = RLock()

        h = object.__new__(Handler)
        h._cached_image_build_records = lambda: []
        return h

    def node(self, name, epoch="one"):
        return SimpleNamespace(
            job_id=name,
            node_id=name,
            node_epoch=epoch,
            node_url="http://" + name,
            capabilities=("image-build",),
            active_sandboxes=0,
        )

    def test_published_local_image_avoids_fleet_scan_and_tracks_replacement(self):
        with TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            h = self.handler()
            h.image_manager = SimpleNamespace(get_image=store.get, store=store)
            h.registry_url = ""
            h.registry_worker_url = ""
            h._cached_raw_image_inventory_across_nodes = Mock(
                side_effect=AssertionError("fleet scan")
            )
            now = utc_now()
            record = ImageRecord("image", "example:v1", "registry", "ready", now, now)
            store.upsert(record)
            store.load = Mock(side_effect=AssertionError("full image scan"))
            for tag in ("example:v1", "example:v2"):
                store.upsert(replace(record, tag=tag))
                self.assertEqual(
                    h._resolve_sandbox_image_reference("image", reference_kind="name"),
                    (tag, None),
                )
            h._cached_raw_image_inventory_across_nodes.assert_not_called()

    def test_deleted_or_unpublished_local_image_uses_discovery(self):
        from ucloud_sandboxes.control_plane import ImageInventorySnapshot

        with TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            h = self.handler()
            h.image_manager = SimpleNamespace(get_image=store.get, store=store)
            h.registry_url = ""
            h.registry_worker_url = ""
            h._cached_raw_image_inventory_across_nodes = Mock(
                return_value=ImageInventorySnapshot.from_records([], complete=False)
            )
            now = utc_now()
            record = ImageRecord(
                "image", "example:v1", "build:local", "ready", now, now
            )
            store.upsert(record)
            for _ in range(2):
                _, error = h._resolve_sandbox_image_reference(
                    "image", reference_kind="name"
                )
                self.assertEqual(error["error_code"], "image_inventory_incomplete")
                store.delete_by_tags([record.tag])
            self.assertEqual(h._cached_raw_image_inventory_across_nodes.call_count, 2)

    def test_local_managed_image_still_requires_digest_protection(self):
        with TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            h = self.handler()
            h.image_manager = SimpleNamespace(get_image=store.get, store=store)
            h.registry_url = "http://registry.example"
            h.registry_worker_url = ""
            h._managed_registry_manifest_digest = Mock(return_value="")
            h._cached_raw_image_inventory_across_nodes = Mock(
                side_effect=AssertionError("fleet scan")
            )
            now = utc_now()
            store.upsert(
                ImageRecord(
                    "image",
                    "registry.example/team/image:v1",
                    "registry",
                    "ready",
                    now,
                    now,
                )
            )
            _, error = h._resolve_sandbox_image_reference(
                "image", reference_kind="name"
            )
            self.assertEqual(
                error["error_code"], "managed_registry_digest_protection_unavailable"
            )
            h._managed_registry_manifest_digest.assert_called_once()

    def test_unchanged_observation_has_no_write_transaction(self):
        with TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            now = utc_now()
            image = ImageRecord(
                "image", "example:latest", "registry", "ready", now, now
            )
            self.assertTrue(store.upsert_if_changed(image))
            original = store._transaction

            def read_only(*, write):
                self.assertFalse(write)
                return original(write=write)

            store._transaction = read_only
            with ThreadPoolExecutor(max_workers=8) as pool:
                self.assertEqual(
                    list(pool.map(lambda _: store.upsert_if_changed(image), range(32))),
                    [False] * 32,
                )
            store._transaction = original
            self.assertTrue(
                store.upsert_if_changed(replace(image, labels={"new": "value"}))
            )
            store.delete_by_tags([image.tag])
            self.assertTrue(store.upsert_if_changed(image))

    def test_concurrent_first_observation_changes_once(self):
        with TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            now = utc_now()
            image = ImageRecord(
                "image", "example:latest", "registry", "ready", now, now
            )
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(
                    pool.map(lambda _: store.upsert_if_changed(image), range(32))
                )
            self.assertEqual(sum(results), 1)

    def test_inventory_invalidated_only_on_changed_image(self):
        with TemporaryDirectory() as directory:
            h = self.handler()
            h.image_manager = SimpleNamespace(
                store=ImageStore(Path(directory) / "images.sqlite")
            )
            h._image_record_with_registry_digest = lambda value: value
            h._invalidate_image_inventory_cache = Mock()
            now = utc_now()
            image = ImageRecord(
                "image", "example:latest", "registry", "ready", now, now
            )
            for _ in range(5):
                h._record_successful_build_image(
                    {"status": "succeeded", "image": image.to_dict()}
                )
            h._invalidate_image_inventory_cache.assert_called_once()
            h._record_successful_build_image(
                {
                    "status": "succeeded",
                    "image": replace(image, labels={"new": "value"}).to_dict(),
                }
            )
            self.assertEqual(h._invalidate_image_inventory_cache.call_count, 2)

    def test_exact_build_poll_uses_owner_and_recovers_when_owner_loses_build(self):
        h = self.handler()
        nodes = [self.node("a"), self.node("b")]
        h._ready_heartbeats = lambda: nodes
        calls = []
        owner = "http://b"

        def fetch(url, path, **kwargs):
            calls.append((url, path))
            self.assertEqual(path, "/v1/images/builds/build-id")
            build = {"build_id": "build-id", "image_id": "image"}
            return SimpleNamespace(
                status=200 if url == owner else 404, json=lambda: {"build": build}
            )

        h._proxy_request = fetch
        self.assertEqual(h._image_build_records_for_key("build-id")[0]["location"], "b")
        calls.clear()
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(
                pool.map(
                    lambda _: h._image_build_records_for_key("build-id"), range(16)
                )
            )
        self.assertEqual({url for url, _ in calls}, {"http://b"})
        owner = "http://a"
        calls.clear()
        self.assertEqual(h._image_build_records_for_key("build-id")[0]["location"], "a")
        self.assertEqual([url for url, _ in calls], ["http://b", "http://a"])

    def test_restarted_owner_does_not_use_old_hint(self):
        h = self.handler()
        h.image_build_owners["build-id"] = ("b", "b", "old")
        h._ready_heartbeats = lambda: [self.node("a"), self.node("b", "new")]
        calls = []

        def fetch(url, path, **kwargs):
            calls.append(url)
            return SimpleNamespace(
                status=200, json=lambda: {"build": {"build_id": "build-id"}}
            )

        h._proxy_request = fetch
        h._image_build_records_for_key("build-id")
        self.assertEqual(calls, ["http://a"])

    def test_image_name_discovers_all_builders_and_selects_latest(self):
        h = self.handler()
        h._ready_heartbeats = lambda: [self.node("a"), self.node("b")]
        calls = []

        def fetch(url, path, **kwargs):
            calls.append(url)
            name = url[-1]
            return SimpleNamespace(
                status=200,
                json=lambda: {
                    "build": {"build_id": name, "image_id": "image", "created_at": name}
                },
            )

        h._proxy_request = fetch
        h._write_json = Mock()
        h._record_successful_build_image = Mock()
        h._get_image_build("image")
        self.assertEqual(calls, ["http://a", "http://b"])
        self.assertEqual(h._write_json.call_args.args[0]["build"]["build_id"], "b")
        self.assertNotIn("image", h.image_build_owners)

    def test_unknown_build_does_not_enrich_unrelated_results(self):
        h = self.handler()
        h._ready_heartbeats = lambda: [self.node("a")]
        h._proxy_request = lambda *a, **kw: SimpleNamespace(
            status=200, json=lambda: {"build": {"build_id": "unrelated"}}
        )
        h._record_successful_build_image = Mock()
        h._write_json = Mock()
        h._get_image_build("absent")
        self.assertEqual(h._write_json.call_args.kwargs["status"], 404)
        h._record_successful_build_image.assert_not_called()
