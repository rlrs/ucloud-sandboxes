from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from ucloud_sandboxes.environment_artifact import ENVIRONMENT_ANNOTATION
from ucloud_sandboxes.managed_registry import (
    RegistryImageUsage,
    RegistryRequestError,
    RegistryTag,
    RegistryUsageStore,
)
from ucloud_sandboxes.registry_retention import (
    ENVIRONMENT_REASON,
    SNAPSHOT_REASON,
    EnvironmentBlobIndex,
    ImageEnvironmentIndex,
    ManagedImage,
    RegistryTagClock,
    environment_live_identities,
    execute_reference_prune,
    managed_images,
    routing_image_identities,
    select_lru_evictions,
    select_unreferenced,
    snapshot_live_identities,
    still_unreferenced_environment,
)


NOW = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
SNAPSHOTS = "ucloud-sandbox-snapshots"
ENVIRONMENTS = "environments"


def digest(character: str) -> str:
    return "sha256:" + character * 64


def at(hours_ago: float):
    return lambda _record: NOW - timedelta(hours=hours_ago)


class FakeRoutingStore:
    def __init__(self, *, dependencies=(), routes=(), migrations=(), incomplete=False,
                 prepared=(), warmups=()):
        self.dependencies = list(dependencies)
        self.routes = list(routes)
        self.migrations = list(migrations)
        self.incomplete = incomplete
        self.prepared = list(prepared)
        self.warmups = list(warmups)

    def storage_snapshot_dependencies_readonly(self, *, require_complete=False):
        if require_complete and self.incomplete:
            raise ValueError("cannot GC before every sandbox reports its storage dependencies")
        return self.dependencies

    def sandbox_routes_readonly(self):
        return self.routes

    def sandbox_migrations(self, *, active_only=False):
        assert active_only
        return self.migrations

    def load(self):
        return SimpleNamespace(
            sandboxes={route.sandbox_id: route for route in self.routes},
            prepared={str(index): item for index, item in enumerate(self.prepared)},
            image_warmups={str(index): item for index, item in enumerate(self.warmups)},
        )


def route(sandbox_id="sandbox", *, tag="", manifest="", storage_snapshot=None, image=""):
    return SimpleNamespace(
        sandbox_id=sandbox_id,
        snapshot_tag=tag,
        snapshot_manifest_digest=manifest,
        storage_snapshot=storage_snapshot or {},
        spec={"image": image} if image else {},
    )


class FakeRegistry:
    """Manifest documents and blobs by (repository, reference)."""

    def __init__(self):
        self.documents: dict[tuple[str, str], dict] = {}
        self.blobs: dict[tuple[str, str], bytes] = {}
        self.deleted: list[tuple[str, str]] = []

    def manifest_document(self, repository, reference):
        try:
            return self.documents[(repository, reference)], {}
        except KeyError:
            raise RegistryRequestError(404, "GET", reference, "{}") from None

    def blob_bytes(self, repository, blob, *, max_bytes):
        return self.blobs[(repository, blob)][:max_bytes]

    def delete_manifest(self, repository, manifest):
        if (repository, manifest) in self.deleted:
            raise RegistryRequestError(404, "DELETE", manifest, "{}")
        self.deleted.append((repository, manifest))

    def add_environment_root(self, root, *, base, toolkits=(), workspace=None):
        config = json.dumps({
            "environment": {"base": base, "toolkits": list(toolkits), "workspace": workspace},
        }).encode()
        config_digest = digest("c")
        self.documents[(ENVIRONMENTS, root)] = {
            "config": {"digest": config_digest, "size": len(config)}, "layers": [],
        }
        self.blobs[(ENVIRONMENTS, config_digest)] = config


class SnapshotLivenessTests(unittest.TestCase):
    def test_routes_dependencies_and_migrations_keep_snapshots_live(self) -> None:
        store = FakeRoutingStore(
            dependencies=[{"publication": {"manifest_digest": digest("1"), "tag": "dep"}}],
            routes=[route(tag="parked", manifest=digest("2"),
                          storage_snapshot={"references": [{"tag": "split-memory"}]})],
            migrations=[SimpleNamespace(storage_snapshot={"publication": {"tag": "moving"}})],
        )

        live = snapshot_live_identities(store)

        self.assertTrue({digest("1"), digest("2"), "dep", "parked", "split-memory",
                         "moving"} <= live)
        self.assertNotIn("", live)

    def test_missing_dependency_report_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "storage dependencies"):
            snapshot_live_identities(FakeRoutingStore(incomplete=True))

    def test_selection_honours_references_grace_leases_and_unknown_age(self) -> None:
        records = [
            RegistryTag(SNAPSHOTS, "old-dead", digest("a")),
            RegistryTag(SNAPSHOTS, "old-live-tag", digest("b")),
            RegistryTag(SNAPSHOTS, "old-live-digest", digest("c")),
            RegistryTag(SNAPSHOTS, "fresh-dead", digest("d")),
            RegistryTag(SNAPSHOTS, "leased", digest("e")),
            RegistryTag(SNAPSHOTS, "unknown-age", digest("f")),
            RegistryTag("other", "old-dead", digest("9")),
        ]
        ages = {"fresh-dead": 0.5, "unknown-age": None}

        def tag_time(record):
            hours = ages.get(record.tag, 3)
            return None if hours is None else NOW - timedelta(hours=hours)

        decision = select_unreferenced(
            records,
            reason=SNAPSHOT_REASON,
            repository=SNAPSHOTS,
            live={"old-live-tag", digest("c")},
            grace_seconds=3600,
            tag_time=tag_time,
            leased_digests={(SNAPSHOTS, digest("e"))},
            now=NOW,
        )

        self.assertEqual([item.tag for item in decision.delete], ["old-dead"])
        self.assertEqual(
            decision.kept, {"live": 2, "grace": 1, "leased": 1, "age_unknown": 1},
        )
        self.assertEqual(decision.to_dict()["delete_manifests"], 1)

    def test_one_young_alias_keeps_the_whole_digest(self) -> None:
        records = [
            RegistryTag(SNAPSHOTS, "old", digest("a")),
            RegistryTag(SNAPSHOTS, "new", digest("a")),
        ]
        decision = select_unreferenced(
            records, reason=SNAPSHOT_REASON, repository=SNAPSHOTS, live=set(),
            grace_seconds=3600,
            tag_time=lambda record: NOW - timedelta(hours=5 if record.tag == "old" else 0.1),
            now=NOW,
        )
        self.assertEqual(decision.delete, ())


class EnvironmentLivenessTests(unittest.TestCase):
    def test_live_roots_come_from_image_annotations_with_their_components(self) -> None:
        registry = FakeRegistry()
        root, base, toolkit, workspace = digest("1"), digest("2"), digest("3"), digest("4")
        registry.documents[("ucloud-managed/a", digest("a"))] = {
            "annotations": {ENVIRONMENT_ANNOTATION: root},
        }
        registry.documents[("ucloud-managed/b", digest("b"))] = {"annotations": {}}
        registry.add_environment_root(root, base=base, toolkits=[toolkit], workspace=workspace)
        index = ImageEnvironmentIndex(registry)

        roots = index.roots([
            RegistryTag("ucloud-managed/a", "latest", digest("a")),
            RegistryTag("ucloud-managed/b", "latest", digest("b")),
            RegistryTag("ucloud-managed/gone", "latest", digest("5")),
        ])
        live = environment_live_identities(registry, ENVIRONMENTS, roots)

        self.assertEqual(roots, {root})
        self.assertEqual(live, {root, base, toolkit, workspace})
        decision = select_unreferenced(
            [
                RegistryTag(ENVIRONMENTS, "environment-root-x", root),
                RegistryTag(ENVIRONMENTS, "environment-component-y", base),
                RegistryTag(ENVIRONMENTS, "environment-component-z", digest("6")),
            ],
            reason=ENVIRONMENT_REASON, repository=ENVIRONMENTS, live=live,
            grace_seconds=3600, tag_time=at(2), now=NOW,
        )
        self.assertEqual([item.digest for item in decision.delete], [digest("6")])

    def test_shared_layer_components_live_while_any_root_lists_them(self) -> None:
        registry = FakeRegistry()
        base_root, task_root, dead_root = digest("1"), digest("2"), digest("3")
        shared, task_layer, dead_layer, orphan = digest("4"), digest("5"), digest("6"), digest("7")
        # Two per-layer images share the base component; a third image is gone.
        registry.documents[("ucloud-managed/base", digest("a"))] = {
            "annotations": {ENVIRONMENT_ANNOTATION: base_root},
        }
        registry.documents[("ucloud-managed/task", digest("b"))] = {
            "annotations": {ENVIRONMENT_ANNOTATION: task_root},
        }
        for root, toolkits in ((base_root, []), (task_root, [task_layer]), (dead_root, [dead_layer])):
            config = json.dumps({"environment": {"base": shared, "toolkits": toolkits,
                                                 "workspace": None}}).encode()
            config_digest = "sha256:" + root[-1] * 63 + "c"
            registry.documents[(ENVIRONMENTS, root)] = {
                "config": {"digest": config_digest, "size": len(config)}, "layers": [],
            }
            registry.blobs[(ENVIRONMENTS, config_digest)] = config
        roots = ImageEnvironmentIndex(registry).roots([
            RegistryTag("ucloud-managed/base", "latest", digest("a")),
            RegistryTag("ucloud-managed/task", "latest", digest("b")),
        ])
        live = environment_live_identities(registry, ENVIRONMENTS, roots)
        self.assertEqual(live, {base_root, task_root, shared, task_layer})

        # Every index tag is days old; the tag itself never keeps a component.
        records = [
            RegistryTag(ENVIRONMENTS, "environment-root-base", base_root),
            RegistryTag(ENVIRONMENTS, "environment-root-task", task_root),
            RegistryTag(ENVIRONMENTS, "environment-root-dead", dead_root),
            RegistryTag(ENVIRONMENTS, "layer-" + "4" * 64, shared),
            RegistryTag(ENVIRONMENTS, "layer-" + "5" * 64, task_layer),
            RegistryTag(ENVIRONMENTS, "layer-" + "6" * 64, dead_layer),
            RegistryTag(ENVIRONMENTS, "layer-" + "7" * 64, orphan),
        ]
        decision = select_unreferenced(
            records, reason=ENVIRONMENT_REASON, repository=ENVIRONMENTS, live=live,
            grace_seconds=3600, tag_time=at(72), now=NOW,
        )
        self.assertEqual({item.digest for item in decision.delete}, {dead_root, dead_layer, orphan})
        self.assertEqual(decision.kept["live"], 4)

        # A builder reusing the orphan re-put its tag after planning.
        retagged = {"layer-" + "7" * 64}
        check = still_unreferenced_environment(
            live, lambda record: NOW if record.tag in retagged else NOW - timedelta(hours=72),
            NOW - timedelta(hours=1),
        )
        deleted = execute_reference_prune(registry, decision, usage_store=None, still_unreferenced=check)
        self.assertEqual({item.digest for item in deleted}, {dead_root, dead_layer})
        self.assertFalse(check(RegistryTag(ENVIRONMENTS, "layer-" + "4" * 64, shared)))
        unknown = still_unreferenced_environment(set(), lambda _record: None, NOW)
        self.assertFalse(unknown(RegistryTag(ENVIRONMENTS, "layer-x", orphan)))

    def test_malformed_live_root_raises_instead_of_deleting(self) -> None:
        registry = FakeRegistry()
        registry.documents[(ENVIRONMENTS, digest("1"))] = {"config": "broken"}
        with self.assertRaises(ValueError):
            environment_live_identities(registry, ENVIRONMENTS, {digest("1")})


class RegistryTagClockTests(unittest.TestCase):
    def test_filesystem_link_time_and_usage_time_take_the_newest(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            link = (root / "docker/registry/v2/repositories/ucloud-managed/a"
                    / "_manifests/tags/latest/current/link")
            link.parent.mkdir(parents=True)
            link.write_text(digest("a"))
            pushed = (NOW - timedelta(hours=5)).timestamp()
            os.utime(link, (pushed, pushed))
            record = RegistryTag("ucloud-managed/a", "latest", digest("a"))

            only_push = RegistryTagClock(root)(record)
            used = RegistryTagClock(root, {
                ("ucloud-managed/a", "latest"): RegistryImageUsage(
                    "ref", "ucloud-managed/a", "latest", (NOW - timedelta(hours=1)).isoformat(),
                ),
            })(record)
            unknown = RegistryTagClock(None)(RegistryTag("x", "y", digest("b")))

        self.assertEqual(only_push, NOW - timedelta(hours=5))
        self.assertEqual(used, NOW - timedelta(hours=1))
        self.assertIsNone(unknown)


def image(name, character, *, hours, blobs, tags=("latest",)):
    repository = f"ucloud-managed/{name}"
    return ManagedImage(
        repository=repository,
        digest=digest(character),
        tags=tuple(RegistryTag(repository, tag, digest(character)) for tag in tags),
        last_used=None if hours is None else NOW - timedelta(hours=hours),
        blobs=blobs,
    )


class LruEvictionTests(unittest.TestCase):
    def test_evicts_oldest_first_until_the_projected_target(self) -> None:
        images = [
            image("new", "1", hours=2, blobs={digest("a"): 100}),
            image("oldest", "2", hours=9, blobs={digest("b"): 100}),
            image("older", "3", hours=5, blobs={digest("c"): 100}),
        ]

        plan = select_lru_evictions(
            images, used_bytes=1000, target_used_bytes=850, grace_seconds=3600, now=NOW,
        )

        self.assertEqual([item.repository for item in plan.evict],
                         ["ucloud-managed/oldest", "ucloud-managed/older"])
        self.assertEqual(plan.projected_freed_bytes, 200)
        self.assertEqual(plan.kept["target_reached"], 1)

    def test_shared_blobs_count_only_with_their_last_owner(self) -> None:
        shared = {digest("s"): 500}
        images = [
            image("a", "1", hours=9, blobs={**shared, digest("a"): 10}),
            image("b", "2", hours=8, blobs={**shared, digest("b"): 10}),
        ]

        plan = select_lru_evictions(
            images, used_bytes=1000, target_used_bytes=600, grace_seconds=3600, now=NOW,
        )

        self.assertEqual(len(plan.evict), 2)
        self.assertEqual(plan.projected_freed_bytes, 520)

    def test_routes_leases_grace_and_unknown_age_protect_images(self) -> None:
        images = [
            image("routed", "1", hours=9, blobs={digest("a"): 100}),
            image("routed-by-tag", "2", hours=9, blobs={digest("b"): 100}),
            image("leased", "3", hours=9, blobs={digest("c"): 100}),
            image("recent", "4", hours=0.5, blobs={digest("d"): 100}),
            image("unknown", "5", hours=None, blobs={digest("e"): 100}),
            image("evictable", "6", hours=9, blobs={digest("f"): 100}),
        ]
        store = FakeRoutingStore(
            routes=[
                route("s1", image=f"registry:5000/ucloud-managed/routed:latest@{digest('1')}"),
                route("s2", image="registry:5000/ucloud-managed/routed-by-tag:latest"),
            ],
        )

        plan = select_lru_evictions(
            images,
            used_bytes=10_000,
            target_used_bytes=0,
            grace_seconds=3600,
            live=routing_image_identities(store),
            leased_digests={("ucloud-managed/leased", digest("3"))},
            now=NOW,
        )

        self.assertEqual([item.repository for item in plan.evict], ["ucloud-managed/evictable"])
        self.assertEqual(
            {key: plan.kept[key] for key in ("live", "leased", "grace", "age_unknown")},
            {"live": 2, "leased": 1, "grace": 1, "age_unknown": 1},
        )

    def test_prepared_capacity_and_warmups_keep_their_images(self) -> None:
        store = FakeRoutingStore(
            prepared=[SimpleNamespace(image="registry:5000/ucloud-managed/p:latest")],
            warmups=[SimpleNamespace(image=f"registry:5000/ucloud-managed/w@{digest('7')}")],
        )
        live = routing_image_identities(store)
        self.assertIn("ucloud-managed/p:latest", live)
        self.assertIn(f"ucloud-managed/w@{digest('7')}", live)

    def test_managed_images_group_aliases_and_include_environment_blobs(self) -> None:
        registry = FakeRegistry()
        root, component = digest("1"), digest("2")
        registry.add_environment_root(root, base=component)
        registry.documents[(ENVIRONMENTS, component)] = {
            "layers": [{"digest": digest("e"), "size": 900}],
        }
        environment_blobs = EnvironmentBlobIndex(registry, ENVIRONMENTS)
        records = [
            RegistryTag("ucloud-managed/a", "latest", digest("a")),
            RegistryTag("ucloud-managed/a", "ucloud-digest-sha256-" + "a" * 64, digest("a")),
            RegistryTag("other/repo", "latest", digest("b")),
        ]

        images = managed_images(
            records,
            tag_time=lambda record: NOW - timedelta(hours=3 if record.tag == "latest" else 2),
            blobs=lambda _repository, _digest: {
                digest("l"): 50, **environment_blobs.blobs(root),
            },
        )

        self.assertEqual(len(images), 1)
        self.assertEqual(len(images[0].tags), 2)
        self.assertEqual(images[0].last_used, NOW - timedelta(hours=2))
        self.assertEqual(images[0].blobs, {digest("l"): 50, digest("e"): 900})


class ExecuteReferencePruneTests(unittest.TestCase):
    def test_leases_touches_and_revalidation_fence_each_delete(self) -> None:
        with TemporaryDirectory() as raw:
            store = RegistryUsageStore(Path(raw) / "usage.sqlite")
            store.acquire_lease(
                "ucloud-managed/leased", "latest", "sandbox:1", ttl_seconds=60,
                digest=digest("2"),
            )
            store.touch_image("registry:5000/ucloud-managed/touched:latest")
            registry = FakeRegistry()
            registry.deleted.append(("ucloud-managed/gone", digest("5")))
            records = [
                RegistryTag("ucloud-managed/free", "latest", digest("1")),
                RegistryTag("ucloud-managed/leased", "latest", digest("2")),
                RegistryTag("ucloud-managed/touched", "latest", digest("3")),
                RegistryTag("ucloud-managed/now-live", "latest", digest("4")),
                RegistryTag("ucloud-managed/gone", "latest", digest("5")),
            ]

            deleted = execute_reference_prune(
                registry,
                records,
                usage_store=store,
                still_unreferenced=lambda record: record.repository != "ucloud-managed/now-live",
                unused_since=datetime.now(timezone.utc) - timedelta(hours=1),
                batch_size=2,
            )

        self.assertEqual([item.repository for item in deleted], ["ucloud-managed/free"])
        self.assertIn(("ucloud-managed/free", digest("1")), registry.deleted)
        self.assertNotIn(("ucloud-managed/leased", digest("2")), registry.deleted)
        self.assertNotIn(("ucloud-managed/touched", digest("3")), registry.deleted)


class RunRegistryPruneTests(unittest.TestCase):
    """The CLI orchestration: age rules for images, references for snapshots."""

    class Client(FakeRegistry):
        tags_by_repository: dict[str, dict[str, str]] = {}
        tag_record_calls: list[str] = []

        def __init__(self, _url: str) -> None:
            super().__init__()
            self.base_url = "http://registry.invalid"

        def catalog(self):
            return list(self.tags_by_repository)

        def tags(self, repository):
            return list(self.tags_by_repository[repository])

        def manifest_digest(self, repository, tag):
            return self.tags_by_repository[repository][tag]

        def tag_record(self, repository, tag):
            self.tag_record_calls.append(repository)
            return RegistryTag(repository, tag, self.manifest_digest(repository, tag))

        def delete_manifest(self, repository, manifest):
            RunRegistryPruneTests.deleted.append((repository, manifest))

    deleted: list[tuple[str, str]] = []

    def test_unreferenced_old_snapshots_are_deleted_and_counted(self) -> None:
        from ucloud_sandboxes import cli
        from ucloud_sandboxes.config import DeploymentConfig
        from unittest.mock import patch

        with TemporaryDirectory() as raw:
            root = Path(raw)
            config_raw = DeploymentConfig.default("project").to_dict()
            config_raw["data_root"] = str(root / "state")
            config_raw["registry_store"]["mount_point"] = str(root)
            config_raw["registry_store"]["data_root"] = str(root / "registry")
            config = DeploymentConfig.from_dict(config_raw)
            tags = {"old-dead": digest("1"), "old-leased": digest("2"), "fresh": digest("3")}
            for tag, age_hours in (("old-dead", 5), ("old-leased", 5), ("fresh", 0.1)):
                link = (config.registry_data_dir() / "docker/registry/v2/repositories"
                        / SNAPSHOTS / "_manifests/tags" / tag / "current/link")
                link.parent.mkdir(parents=True)
                link.write_text(tags[tag])
                stamp = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).timestamp()
                os.utime(link, (stamp, stamp))
            RegistryUsageStore(config.registry_usage_file()).acquire_reference(
                SNAPSHOTS, "old-leased", "route:sandbox", digest=digest("2"),
            )
            self.Client.tags_by_repository = {SNAPSHOTS: tags, "ucloud-managed/a": {}}
            self.Client.tag_record_calls = []
            RunRegistryPruneTests.deleted = []

            with patch.object(cli, "RegistryClient", self.Client):
                dry_run = cli.run_registry_prune(config, execute=False)
                self.assertEqual(RunRegistryPruneTests.deleted, [])
                executed = cli.run_registry_prune(config, execute=True)
            state = json.loads(config.registry_maintenance_state_file().read_text())

        decision = dry_run["reference_retention"]["decisions"][0]
        self.assertEqual(decision["reason"], SNAPSHOT_REASON)
        self.assertEqual(decision["delete_manifests"], 1)
        self.assertEqual(decision["kept_manifests"]["leased"], 1)
        self.assertEqual(decision["kept_manifests"]["grace"], 1)
        self.assertEqual(RunRegistryPruneTests.deleted, [(SNAPSHOTS, digest("1"))])
        self.assertEqual(executed["deleted_manifest_count"], 1)
        self.assertEqual(state["deleted_since_gc"], 1)
        # The snapshot repository never goes through the per-tag age scan.
        self.assertNotIn(SNAPSHOTS, self.Client.tag_record_calls)

    def test_pressure_eviction_removes_lru_images_and_their_gateway_records(self) -> None:
        from ucloud_sandboxes import cli
        from ucloud_sandboxes.config import DeploymentConfig
        from ucloud_sandboxes.images import ImageRecord, ImageStore
        from ucloud_sandboxes.registry_disk import measure_registry_disk
        from unittest.mock import patch

        class Client(self.Client):
            def tag_exists(self, repository, tag):
                return (repository, self.tags_by_repository[repository][tag]) not in (
                    RunRegistryPruneTests.deleted
                )

        with TemporaryDirectory() as raw:
            root = Path(raw)
            config_raw = DeploymentConfig.default("project").to_dict()
            config_raw["data_root"] = str(root / "state")
            config_raw["registry_store"]["mount_point"] = str(root)
            config_raw["registry_store"]["data_root"] = str(root / "registry")
            config_raw["snapshot_store"]["kind"] = "registry"
            config = DeploymentConfig.from_dict(config_raw)
            images = {"old": (digest("1"), 9), "older": (digest("2"), 20), "new": (digest("3"), 0.2)}
            Client.tags_by_repository = {
                f"ucloud-managed/{name}": {"latest": image_digest}
                for name, (image_digest, _age) in images.items()
            }
            Client.tag_record_calls = []
            RunRegistryPruneTests.deleted = []
            documents = {}
            for name, (image_digest, age_hours) in images.items():
                repository = f"ucloud-managed/{name}"
                link = (config.registry_data_dir() / "docker/registry/v2/repositories"
                        / repository / "_manifests/tags/latest/current/link")
                link.parent.mkdir(parents=True)
                link.write_text(image_digest)
                stamp = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).timestamp()
                os.utime(link, (stamp, stamp))
                documents[(repository, image_digest)] = {
                    "config": {"digest": digest("c"), "size": 1},
                    "layers": [{"digest": image_digest.replace("sha256:", "sha256:f")[:71],
                                "size": 300 * 1024**3}],
                }
            now = datetime.now(timezone.utc)
            store = ImageStore(config.image_file())
            for name in images:
                store.upsert(ImageRecord(
                    id=f"task-{name}", tag=f"registry:5000/ucloud-managed/{name}:latest",
                    source="build:test", state="ready", created_at=now, updated_at=now,
                    pushed=True, manifest_digest=images[name][0],
                ))
            # 850 of 1000 GiB used; the target is 60 %, so 250 GiB must go:
            # one 300 GiB image, the least recently used one.
            usage = measure_registry_disk(
                ("/registry",), cleanup_percent=70, refuse_percent=90, target_percent=60,
                statvfs=lambda _path: SimpleNamespace(
                    f_frsize=1024**3, f_bsize=1024**3, f_blocks=1000, f_bfree=150,
                    f_bavail=150,
                ),
            )

            def client_factory(url):
                client = Client(url)
                client.documents.update(documents)
                return client

            with (
                patch.object(cli, "RegistryClient", client_factory),
                patch.object(cli, "registry_disk_usage", return_value=usage),
            ):
                result = cli.run_registry_prune(config, execute=True, evict_lru=True)
            remaining = {record.id for record in ImageStore(config.image_file()).load().values()}
            state = json.loads(config.registry_maintenance_state_file().read_text())

        self.assertEqual(result["lru_eviction"]["evicted_images"], 1)
        self.assertEqual(RunRegistryPruneTests.deleted, [("ucloud-managed/older", digest("2"))])
        self.assertEqual(remaining, {"task-old", "task-new"})
        self.assertIn("task-older", state["evicted_images"])
        self.assertIn("last_eviction_at", state)
        self.assertEqual(result["deleted_manifest_count"], 1)


if __name__ == "__main__":
    unittest.main()
