from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import RLock
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from ucloud_sandboxes.control_plane import (
    ControlPlaneHandler,
    ProxiedResponse,
)
from ucloud_sandboxes.gateway.node_rpc import _node_transport_error_response
from ucloud_sandboxes.images import ImageRecord, ImageStore
from ucloud_sandboxes.models import utc_now
from tests.gateway_support import gateway_services


class ImagePollingTests(unittest.TestCase):
    def handler(self):
        class Handler(ControlPlaneHandler):
            image_build_owners = OrderedDict()
            image_build_owners_lock = RLock()
            image_build_metrics_seen = OrderedDict()

        h = object.__new__(Handler)
        h.services = gateway_services()
        h._cached_image_build_records = lambda: []
        return h

    def test_terminal_build_timings_survive_builder_removal_without_log_payloads(self):
        from ucloud_sandboxes.metrics import MetricsStore
        with TemporaryDirectory() as directory:
            h = self.handler()
            path = Path(directory) / "metrics.sqlite"
            h.metrics_store = MetricsStore(path)
            build = {"build_id": "build-1", "image_id": "image-1", "status": "failed",
                     "updated_at": "2026-09-28T08:00:00+00:00",
                     "log_tail": "private build output", "command": ["private command"],
                     "timings": {"total_ms": 100, "phases": {"docker_build_and_push_ms": 90},
                                 "environment": {"groups_reused": 3, "docker_pull_skipped": 1}}}
            with ThreadPoolExecutor(4) as pool:
                list(pool.map(lambda _: h._record_terminal_build_metrics(build), range(12)))
            # Read a fresh store, without a builder or the original handler.
            events = MetricsStore(path).load_events(kinds=("image_build_completed",))
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].data["timings"], build["timings"])
            self.assertEqual(events[0].data["status"], "failed")
            self.assertNotIn("log_tail", events[0].data)
            self.assertNotIn("command", events[0].data)

    def test_failed_metric_write_can_be_retried_without_failing_a_build(self):
        h = self.handler()
        h.metrics_store = Mock()
        h.metrics_store.append.side_effect = [OSError("disk unavailable"), None]
        build = {"build_id": "build-1", "status": "succeeded", "timings": {"total_ms": 12}}
        h._record_terminal_build_metrics(build)
        h._record_terminal_build_metrics(build)
        h._record_terminal_build_metrics(build)
        self.assertEqual(h.metrics_store.append.call_count, 2)

    def test_history_write_retries_independently_of_metrics_deduplication(self):
        h = self.handler()
        h.metrics_store = Mock()
        h.build_history = Mock()
        h.build_history.record.side_effect = [OSError("disk unavailable"), True]
        build = {"build_id": "build-1", "status": "succeeded", "timings": {"total_ms": 12}}
        h._record_terminal_build_metrics(build)
        h._record_terminal_build_metrics(build)
        self.assertEqual(h.metrics_store.append.call_count, 1)
        self.assertEqual(h.build_history.record.call_count, 2)

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
            images = gateway_services(
                image_manager=SimpleNamespace(get_image=store.get, store=store),
            ).images
            images.cached_raw_inventory = Mock(side_effect=AssertionError("fleet scan"))
            now = utc_now()
            record = ImageRecord("image", "example:v1", "registry", "ready", now, now)
            store.upsert(record)
            store.load = Mock(side_effect=AssertionError("full image scan"))
            for tag in ("example:v1", "example:v2"):
                store.upsert(replace(record, tag=tag))
                self.assertEqual(
                    images.resolve(None, "image", reference_kind="name"), (tag, None),
                )
            images.cached_raw_inventory.assert_not_called()

    def test_deleted_or_unpublished_local_image_uses_discovery(self):
        from ucloud_sandboxes.image_inventory_cache import ImageInventorySnapshot

        with TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            images = gateway_services(
                image_manager=SimpleNamespace(get_image=store.get, store=store),
            ).images
            images.cached_raw_inventory = Mock(
                return_value=ImageInventorySnapshot.from_records([], complete=False)
            )
            now = utc_now()
            record = ImageRecord(
                "image", "example:v1", "build:local", "ready", now, now
            )
            store.upsert(record)
            for _ in range(2):
                _, error = images.resolve(None, "image", reference_kind="name")
                self.assertEqual(error["error_code"], "image_inventory_incomplete")
                store.delete_by_tags([record.tag])
            self.assertEqual(images.cached_raw_inventory.call_count, 2)

    def test_local_managed_image_still_requires_digest_protection(self):
        with TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            images = gateway_services(
                image_manager=SimpleNamespace(get_image=store.get, store=store),
                registry_url="http://registry.example", registry_worker_url="",
            ).images
            images.managed_manifest_digest = Mock(return_value="")
            images.cached_raw_inventory = Mock(side_effect=AssertionError("fleet scan"))
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
            _, error = images.resolve(None, "image", reference_kind="name")
            self.assertEqual(
                error["error_code"], "managed_registry_digest_protection_unavailable"
            )
            images.managed_manifest_digest.assert_called_once()

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
            h.services.images.record_with_digest = lambda value: value
            h.services.images.invalidate_inventory = Mock()
            now = utc_now()
            image = ImageRecord(
                "image", "example:latest", "registry", "ready", now, now
            )
            for _ in range(5):
                h._record_successful_build_image(
                    {"status": "succeeded", "image": image.to_dict()}
                )
            h.services.images.invalidate_inventory.assert_called_once()
            h._record_successful_build_image(
                {
                    "status": "succeeded",
                    "image": replace(image, labels={"new": "value"}).to_dict(),
                }
            )
            self.assertEqual(h.services.images.invalidate_inventory.call_count, 2)

    def test_exact_build_poll_uses_owner_and_recovers_when_owner_loses_build(self):
        h = self.handler()
        nodes = [self.node("a"), self.node("b")]
        h.services.fleet.ready_heartbeats = lambda: nodes
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
        h.services.fleet.ready_heartbeats = lambda: [self.node("a"), self.node("b", "new")]
        calls = []

        def fetch(url, path, **kwargs):
            calls.append(url)
            return SimpleNamespace(
                status=200, json=lambda: {"build": {"build_id": "build-id"}}
            )

        h._proxy_request = fetch
        h._image_build_records_for_key("build-id")
        self.assertEqual(calls, ["http://a"])

    def test_owner_timeout_is_retryable_without_fanout_and_recovers(self):
        h = self.handler()
        h.services.fleet.ready_heartbeats = lambda: [self.node("owner"), self.node("peer")]
        h._write_json = Mock()
        h._record_successful_build_image = Mock()
        running = {"build_id": "build-id", "image_id": "image", "status": "running"}
        success = SimpleNamespace(status=200, json=lambda: {"build": running})
        h._proxy_request = Mock(side_effect=[
            success,
            _node_transport_error_response(TimeoutError("fixture timeout")),
            success,
        ])

        h._get_image_build("build-id")
        self.assertEqual(h.image_build_owners["build-id"], ("owner", "owner", "one"))
        h._get_image_build("build-id")
        response = h._write_json.call_args
        self.assertEqual(response.kwargs["status"], 504)
        self.assertTrue(response.args[0]["retryable"])
        self.assertEqual(response.args[0]["error_code"], "image_build_status_unavailable")
        self.assertEqual(response.kwargs["headers"]["Retry-After"], "2")
        h._get_image_build("build-id")
        self.assertEqual(h._write_json.call_args.args[0]["build"]["status"], "running")
        self.assertEqual([call.args[0] for call in h._proxy_request.call_args_list],
                         ["http://owner"] * 3)

    def test_failed_owner_statuses_are_not_absence(self):
        for upstream, expected in [(408, 408), (429, 429), (500, 500),
                                   (502, 502), (503, 503), (400, 502), (403, 502)]:
            with self.subTest(upstream=upstream):
                h = self.handler()
                h.image_build_owners["build-id"] = ("owner", "owner", "one")
                h.services.fleet.ready_heartbeats = lambda: [self.node("owner"), self.node("peer")]
                h._proxy_request = Mock(return_value=ProxiedResponse(
                    upstream, {}, b'{"error":"private upstream detail","retryable":false}'
                ))
                h._write_json = Mock()
                h._get_image_build("build-id")
                response = h._write_json.call_args
                self.assertEqual(response.kwargs["status"], expected)
                self.assertTrue(response.args[0]["retryable"])
                self.assertNotIn("private", response.args[0]["error"])
                h._proxy_request.assert_called_once()

    def test_missing_owner_heartbeat_is_retryable_without_peer_scan(self):
        h = self.handler()
        h.image_build_owners["build-id"] = ("owner", "owner", "one")
        h.services.fleet.ready_heartbeats = lambda: [self.node("peer")]
        h._proxy_request = Mock()
        h._write_json = Mock()
        h._get_image_build("build-id")
        self.assertEqual(h._write_json.call_args.kwargs["status"], 503)
        self.assertTrue(h._write_json.call_args.args[0]["retryable"])
        h._proxy_request.assert_not_called()

    def test_confirmed_absence_returns_404_after_owner_and_fallback_probes(self):
        h = self.handler()
        h.image_build_owners["build-id"] = ("owner", "owner", "one")
        h.services.fleet.ready_heartbeats = lambda: [self.node("owner"), self.node("peer")]
        h._proxy_request = Mock(return_value=ProxiedResponse(404, {}, b"{}"))
        h._write_json = Mock()
        h._get_image_build("build-id")
        self.assertEqual(h._write_json.call_args.kwargs["status"], 404)
        self.assertEqual([call.args[0] for call in h._proxy_request.call_args_list],
                         ["http://owner", "http://peer"])

    def test_failed_discovery_does_not_hide_exact_fallback_or_claim_absence(self):
        for peer_found in (True, False):
            with self.subTest(peer_found=peer_found):
                h = self.handler()
                h.services.fleet.ready_heartbeats = lambda: [self.node("a"), self.node("b")]
                h._proxy_request = Mock(side_effect=[
                    ProxiedResponse(503, {}, b"{}"),
                    SimpleNamespace(
                        status=200 if peer_found else 404,
                        json=lambda: {"build": {"build_id": "build-id", "status": "running"}},
                    ),
                ])
                h._write_json = Mock()
                h._record_successful_build_image = Mock()
                h._get_image_build("build-id")
                response = h._write_json.call_args
                if peer_found:
                    self.assertEqual(response.args[0]["build"]["location"], "b")
                else:
                    self.assertEqual(response.kwargs["status"], 503)
                    self.assertTrue(response.args[0]["retryable"])

    def test_exact_terminal_local_record_survives_unavailable_owner(self):
        for owner_ready in (True, False):
            with self.subTest(owner_ready=owner_ready):
                h = self.handler()
                h.image_build_owners["build-id"] = ("owner", "owner", "one")
                h.services.fleet.ready_heartbeats = lambda: [self.node("owner")] if owner_ready else []
                terminal = {"build_id": "build-id", "image_id": "image", "status": "succeeded"}
                h._cached_image_build_records = lambda: [terminal]
                h._proxy_request = Mock(return_value=ProxiedResponse(504, {}, b"{}"))
                h.routing_store = Mock()
                h._write_json = Mock()
                h._record_successful_build_image = Mock()
                h._get_image_build("build-id")
                self.assertEqual(h._write_json.call_args.args[0], {"build": terminal})
                h.routing_store.clear_pending_image_build.assert_called_once_with("image")

    def test_image_name_cannot_use_older_terminal_record_during_incomplete_scan(self):
        h = self.handler()
        h.services.fleet.ready_heartbeats = lambda: [self.node("a"), self.node("b")]
        old = {"build_id": "older", "image_id": "image", "status": "succeeded"}
        h._cached_image_build_records = lambda: [old]
        h._proxy_request = Mock(side_effect=[
            SimpleNamespace(status=200, json=lambda: {"build": old}),
            ProxiedResponse(503, {}, b"{}"),
        ])
        h._write_json = Mock()
        h._record_successful_build_image = Mock()
        h._get_image_build("image")
        self.assertEqual(h._write_json.call_args.kwargs["status"], 503)
        h._record_successful_build_image.assert_not_called()

    def test_malformed_success_is_retryable_not_confirmed_absence(self):
        for body in (b"not-json", b"{}", b'{"build":[]}'):
            with self.subTest(body=body):
                h = self.handler()
                h.services.fleet.ready_heartbeats = lambda: [self.node("a")]
                h._proxy_request = Mock(return_value=ProxiedResponse(200, {}, body))
                h._write_json = Mock()
                h._get_image_build("build-id")
                self.assertEqual(h._write_json.call_args.kwargs["status"], 502)
                self.assertTrue(h._write_json.call_args.args[0]["retryable"])

    def test_image_name_discovers_all_builders_and_selects_latest(self):
        h = self.handler()
        h.services.fleet.ready_heartbeats = lambda: [self.node("a"), self.node("b")]
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
        h.services.fleet.ready_heartbeats = lambda: [self.node("a")]
        h._proxy_request = lambda *a, **kw: SimpleNamespace(
            status=200, json=lambda: {"build": {"build_id": "unrelated"}}
        )
        h._record_successful_build_image = Mock()
        h._write_json = Mock()
        h._get_image_build("absent")
        self.assertEqual(h._write_json.call_args.kwargs["status"], 502)
        self.assertTrue(h._write_json.call_args.args[0]["retryable"])
        h._record_successful_build_image.assert_not_called()
