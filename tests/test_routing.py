from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import json
import unittest

from ucloud_sandboxes.models import ResourceQuantity, utc_now
from ucloud_sandboxes.routing import (
    PreparedCapacityDemand,
    RoutingState,
    RoutingStore,
    SandboxRoute,
)


class RoutingStoreTests(unittest.TestCase):
    def test_concurrent_writes_preserve_valid_state(self) -> None:
        with TemporaryDirectory() as raw_dir:
            route_file = Path(raw_dir) / "routes.json"

            def write(index: int) -> None:
                store = RoutingStore(route_file)
                store.upsert_pending(
                    f"pending-{index}",
                    ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
                )
                store.upsert_sandbox(
                    SandboxRoute(
                        sandbox_id=f"sandbox-{index}",
                        node_id="node-1",
                        job_id="job-1",
                        node_url="http://node-1:8090",
                        resources=ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
                    )
                )

            with ThreadPoolExecutor(max_workers=16) as executor:
                list(executor.map(write, range(32)))

            state = RoutingStore(route_file).load()

        self.assertEqual(len(state.pending), 32)
        self.assertEqual(len(state.sandboxes), 32)
        self.assertEqual(
            state.pending["pending-0"].resources,
            ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
        )

    def test_legacy_json_file_is_moved_aside(self) -> None:
        with TemporaryDirectory() as raw_dir:
            route_file = Path(raw_dir) / "routes.json"
            route_file.write_text(
                json.dumps(
                    {
                        "sandboxes": [],
                        "exec_sessions": [],
                        "pending": [],
                        "image_builds": [],
                    }
                ),
                encoding="utf-8",
            )

            state = RoutingStore(route_file).load()
            backups = list(Path(raw_dir).glob("routes.json.legacy-*"))

        self.assertEqual(state.pending, {})
        self.assertEqual(len(backups), 1)

    def test_prepared_capacity_contributes_to_demand_until_deleted(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")

            prepared = store.upsert_prepared_capacity(
                "prep-1",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=4,
                ttl_seconds=600,
            )
            demand = store.pending_demand()
            deleted = store.delete_prepared_capacity("prep-1")
            demand_after_delete = store.pending_demand()

        self.assertEqual(prepared.total_resources.vcpu, 4.0)
        self.assertEqual(demand.pending_resources, ResourceQuantity())
        self.assertEqual(
            demand.prepared_resources,
            ResourceQuantity(vcpu=4, memory_mb=2048, disk_mb=4096),
        )
        self.assertEqual(demand.desired_resources, demand.prepared_resources)
        self.assertEqual(deleted.prepare_id if deleted else None, "prep-1")
        self.assertEqual(demand_after_delete.prepared_resources, ResourceQuantity())

    def test_expired_prepared_capacity_is_pruned_from_demand(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.save(
                RoutingState(
                    sandboxes={},
                    exec_sessions={},
                    pending={},
                    image_builds={},
                    prepared={
                        "expired": PreparedCapacityDemand(
                            prepare_id="expired",
                            resources=ResourceQuantity(vcpu=2, memory_mb=1024),
                            count=2,
                            created_at=(now - timedelta(seconds=30)).isoformat(),
                            updated_at=(now - timedelta(seconds=30)).isoformat(),
                            expires_at=(now - timedelta(seconds=1)).isoformat(),
                        )
                    },
                )
            )

            demand = store.pending_demand()
            state = store.load()

        self.assertEqual(demand.prepared_resources, ResourceQuantity())
        self.assertEqual(state.prepared, {})


if __name__ == "__main__":
    unittest.main()
