from dataclasses import replace
import unittest

from ucloud_sandboxes.models import (
    InstancePhase,
    NodeHeartbeat,
    ResourceQuantity,
    SandboxInventoryEntry,
    SandboxNode,
    ProviderInstance,
    utc_now,
)
from ucloud_sandboxes.reconcile import (
    node_drain_ready,
    partition_safe_stop_job_ids,
)


class ReconcileTests(unittest.TestCase):
    def test_drain_ready_requires_matching_complete_zero_work_epoch(self) -> None:
        now = utc_now()
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=now,
            active_sandboxes=0,
            active_image_builds=0,
            draining=True,
            inventory_complete=True,
            activity_epoch=7,
            drain_token="drain-1",
            drain_activity_epoch=7,
            admission_open=False,
        )
        node = SandboxNode(
            job=ProviderInstance(
                id="job-1",
                name="node-1",
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu",
                product_category="cpu",
                state="RUNNING",
                phase=InstancePhase.RUNNING,
            ),
            heartbeat=heartbeat,
            active_sandboxes=0,
            heartbeat_fresh=True,
        )

        self.assertTrue(node_drain_ready(node, "drain-1"))
        self.assertFalse(node_drain_ready(node, "other"))
        self.assertFalse(
            node_drain_ready(replace(node, heartbeat_fresh=False), "drain-1")
        )
        for changed in (
            replace(heartbeat, inventory_complete=False),
            replace(heartbeat, admission_open=True),
            replace(heartbeat, drain_activity_epoch=6),
            replace(heartbeat, reserved_resources=ResourceQuantity(vcpu=1)),
            replace(heartbeat, build_reserved_resources=ResourceQuantity(memory_mb=1)),
            replace(heartbeat, used_resources=ResourceQuantity(disk_mb=1)),
            replace(
                heartbeat,
                inventory=(
                    SandboxInventoryEntry(
                        sandbox_id="sandbox-1",
                        generation=1,
                        operation_id="create-1",
                        spec_hash="a" * 64,
                        state="running",
                    ),
                ),
            ),
        ):
            self.assertFalse(
                node_drain_ready(replace(node, heartbeat=changed), "drain-1")
            )

    def test_partitions_stop_job_ids_by_deployment_label(self) -> None:
        class Node:
            def __init__(self, job):
                self.job = job

        owned = ProviderInstance(
            id="job-1",
            name="ucloud-sandbox-node-1",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="RUNNING",
            phase=InstancePhase.RUNNING,
            labels={
                "ucloud-sandboxes/node": "true",
                "ucloud-sandboxes/deployment": "prod-a",
            },
        )
        foreign = ProviderInstance(
            id="job-2",
            name="ucloud-sandbox-node-2",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="RUNNING",
            phase=InstancePhase.RUNNING,
            labels={
                "ucloud-sandboxes/node": "true",
                "ucloud-sandboxes/deployment": "prod-b",
            },
        )

        safe, blocked = partition_safe_stop_job_ids(
            [Node(owned), Node(foreign)],
            ("job-1", "job-2", "job-3"),
            deployment_id="prod-a",
        )

        self.assertEqual(safe, ("job-1",))
        self.assertEqual(blocked, ("job-2", "job-3"))


if __name__ == "__main__":
    unittest.main()
