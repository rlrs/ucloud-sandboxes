from dataclasses import replace
from types import SimpleNamespace
import unittest

from tests import test_control_plane as fixtures
from ucloud_sandboxes.capabilities import (
    HIBERNATE_LOCAL_CAPABILITY, HOST_EROFS_CAPABILITY,
    RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX, STORAGE_NATIVE_CAPABILITY,
    STORAGE_NATIVE_MIGRATION_CAPABILITY,
)
from ucloud_sandboxes.control_plane import ControlPlaneHandler
from ucloud_sandboxes.models import NodeHeartbeat, NodeRuntimeMetrics, ResourceQuantity, utc_now


class MigrationRuntimeEligibilityTests(unittest.TestCase):
    def heartbeat(self, node, *extra):
        return NodeHeartbeat(
            node_id=node, job_id=node, deployment_id="deployment",
            updated_at=utc_now(), active_sandboxes=0,
            node_url=f"http://{node}:8090", resources_known=True,
            total_resources=ResourceQuantity(vcpu=8, memory_mb=16_384, disk_mb=100_000),
            runtime_metrics=NodeRuntimeMetrics(collected_at=utc_now(), cpu_count=8,
                                              storage_hard_capacity_mb=100_000),
            capabilities=("sandbox", "disk-quota", HIBERNATE_LOCAL_CAPABILITY,
                          STORAGE_NATIVE_CAPABILITY, STORAGE_NATIVE_MIGRATION_CAPABILITY,
                          *extra),
        )

    def source(self, *, detached=False, published=False):
        snapshot = fixtures._portable_snapshot("sandbox")
        kwargs = {}
        if published:
            kwargs = dict(
                storage_schema=snapshot.schema,
                snapshot_manifest_digest=snapshot.reference.manifest_digest,
                snapshot_repository=snapshot.reference.repository,
                snapshot_tag=snapshot.reference.tag,
                storage_snapshot=snapshot.to_dict(),
            )
        route = fixtures._sandbox_route(
            sandbox_id="sandbox", node_id="source", job_id="source",
            node_url="http://source:8090", state="parked",
            worker_state="detached" if detached else "attached",
            spec=snapshot.manifest.spec.to_dict(),
            resources=snapshot.manifest.spec.requested_resources(), **kwargs,
        )
        return route, RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + snapshot.manifest.runtime.node_compatibility_sha256

    def select(self, route, destinations, owner=None):
        handler = object.__new__(ControlPlaneHandler)
        handler.heartbeat_ttl_seconds = 120
        handler._placement_routes = lambda: [route]
        handler.routing_store = SimpleNamespace(sandbox_migrations=lambda **_: [])
        handler._ready_sandbox_heartbeats = lambda **_kwargs: destinations
        handler._heartbeat_for_route = lambda **_: owner
        return handler._select_migration_destination(route, requested_node_id="")

    def test_source_less_checkpoint_requires_its_exact_advertised_runtime(self):
        route, capability = self.source(detached=True, published=True)
        compatible = self.heartbeat("matching", capability)
        wrong_abi = self.heartbeat("wrong", RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + "f" * 64)
        legacy = self.heartbeat("legacy")
        self.assertEqual(self.select(route, [wrong_abi, legacy, compatible]), compatible)
        self.assertIsNone(self.select(route, [wrong_abi, legacy]))

    def test_published_runtime_is_authority_even_when_former_owner_changed(self):
        route, capability = self.source(published=True)
        source = self.heartbeat("source", RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + "f" * 64)
        matching = self.heartbeat("matching", capability, HOST_EROFS_CAPABILITY)
        self.assertEqual(self.select(route, [matching], source), matching)
        broken = replace(route, storage_snapshot={**route.storage_snapshot, "unexpected": True})
        self.assertIsNone(self.select(broken, [matching], source))

    def test_unpublished_attached_source_uses_advertised_runtime(self):
        route, capability = self.source()
        source = self.heartbeat("source", capability, HOST_EROFS_CAPABILITY)
        matching = self.heartbeat("matching", capability, HOST_EROFS_CAPABILITY)
        legacy = self.heartbeat("legacy")
        self.assertEqual(self.select(route, [legacy, matching], source), matching)
        self.assertIsNone(self.select(route, [legacy], source))

    def test_attached_legacy_docker_keeps_legacy_path_but_excludes_erofs(self):
        route, capability = self.source()
        source = self.heartbeat("source")
        legacy = self.heartbeat("legacy")
        erofs = self.heartbeat("erofs", capability, HOST_EROFS_CAPABILITY)
        self.assertEqual(self.select(route, [erofs, legacy], source), legacy)
        self.assertIsNone(self.select(route, [erofs], source))

    def test_missing_or_ambiguous_new_source_identity_is_not_legacy(self):
        route, capability = self.source()
        target = self.heartbeat("target", capability)
        for extra in [(HOST_EROFS_CAPABILITY,),
                      (RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + "invalid",),
                      (capability, RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + "f" * 64)]:
            with self.subTest(extra=extra):
                self.assertIsNone(self.select(route, [target], self.heartbeat("source", *extra)))
