from datetime import timedelta
import unittest

from ucloud_sandboxes.config import AutoscalerConfig
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxDemand,
    SandboxNode,
    ScaleAction,
    ScaleDecision,
    ScalePolicy,
    VmJob,
    utc_now,
)
from ucloud_sandboxes.policy import evaluate_scale
from ucloud_sandboxes.reconcile import (
    VmNodeSubmissionDefaults,
    build_builder_vm_create_intents,
    build_vm_create_intents,
    bulk_payload_from_create_intents,
    evaluate_builder_scale,
    partition_safe_stop_job_ids,
    stop_job_ids_from_decision,
)
from ucloud_sandboxes.vm_submit import VmProductRef


class ReconcileTests(unittest.TestCase):
    def test_builder_scale_creates_for_pending_image_build(self) -> None:
        decision = evaluate_builder_scale(
            [],
            pending_builds=1,
            policy=ScalePolicy(max_create_per_cycle=5),
            max_builder_nodes=1,
        )

        self.assertEqual(decision.creates, 1)
        self.assertIn("pending image build", decision.reasons[0])

    def test_builder_scale_stops_idle_builder_when_no_build_demand(self) -> None:
        now = utc_now()
        builder = SandboxNode(
            job=VmJob(
                id="builder-1",
                project_id="project-1",
                name="ucloud-sandbox-builder-1",
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu-amd-zen5-16-vcpu",
                product_category="cpu-amd-zen5",
                state="RUNNING",
                started_at=now - timedelta(seconds=600),
                labels={
                    "ucloud-sandboxes/builder": "true",
                    "ucloud-sandboxes/deployment": "prod-a",
                },
            ),
            heartbeat=NodeHeartbeat(
                node_id="builder-1",
                job_id="builder-1",
                updated_at=now,
                active_sandboxes=0,
                idle_since=now - timedelta(seconds=600),
                capabilities=("image-cache", "image-build", "snapshot"),
            ),
            active_sandboxes=0,
            heartbeat_fresh=True,
        )

        decision = evaluate_builder_scale(
            [builder],
            pending_builds=0,
            policy=ScalePolicy(
                max_stop_per_cycle=1,
                scale_down_idle_seconds=0,
                builder_scale_down_idle_seconds=300,
            ),
            max_builder_nodes=1,
            now=now,
        )

        self.assertEqual(decision.stops, ("builder-1",))

    def test_builder_scale_waits_for_builder_idle_grace(self) -> None:
        now = utc_now()
        builder = SandboxNode(
            job=VmJob(
                id="builder-1",
                project_id="project-1",
                name="ucloud-sandbox-builder-1",
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu-amd-zen5-16-vcpu",
                product_category="cpu-amd-zen5",
                state="RUNNING",
                started_at=now - timedelta(seconds=120),
                labels={
                    "ucloud-sandboxes/builder": "true",
                    "ucloud-sandboxes/deployment": "prod-a",
                },
            ),
            heartbeat=NodeHeartbeat(
                node_id="builder-1",
                job_id="builder-1",
                updated_at=now,
                active_sandboxes=0,
                capabilities=("image-cache", "image-build", "snapshot"),
            ),
            active_sandboxes=0,
            heartbeat_fresh=True,
        )

        decision = evaluate_builder_scale(
            [builder],
            pending_builds=0,
            policy=ScalePolicy(
                max_stop_per_cycle=1,
                scale_down_idle_seconds=0,
                builder_scale_down_idle_seconds=900,
            ),
            max_builder_nodes=1,
            now=now,
        )

        self.assertEqual(decision.stops, ())
        self.assertEqual(decision.reasons, ("builder pool matches demand and policy",))

    def test_builds_builder_vm_create_intents_without_node_label(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            deployment_id="prod-a",
            private_network_id="net-1",
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        decision = evaluate_builder_scale(
            [],
            pending_builds=1,
            policy=ScalePolicy(max_create_per_cycle=5),
            max_builder_nodes=1,
        )

        intents = build_builder_vm_create_intents(
            config,
            decision,
            VmNodeSubmissionDefaults(
                private_network_id=config.private_network_id,
                product=VmProductRef(
                    id="cpu-amd-zen5-16-vcpu",
                    category="cpu-amd-zen5",
                    provider="ucloud",
                ),
                disk_gb=250,
            ),
            seed_prefix="cycle-1",
        )

        item = intents[0].options.job_item()
        self.assertEqual(intents[0].node_id, "sandbox-builder-cycle-1-builder-1")
        self.assertEqual(item["labels"]["ucloud-sandboxes/builder"], "true")
        self.assertNotIn("ucloud-sandboxes/node", item["labels"])
        self.assertEqual(item["product"]["id"], "cpu-amd-zen5-16-vcpu")

    def test_builds_vm_create_intents_from_scale_decision(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            deployment_id="prod-a",
            private_network_id="net-1",
            gateway_public_link_id="link-gateway",
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        decision = evaluate_scale(
            [],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=4, memory_mb=12_288)),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )

        intents = build_vm_create_intents(
            config,
            decision,
            VmNodeSubmissionDefaults(private_network_id=config.private_network_id),
            seed_prefix="cycle-1",
        )

        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].seed, "cycle-1-1")
        self.assertEqual(intents[0].node_id, "sandbox-node-cycle-1-1")
        self.assertEqual(intents[0].node_url, "http://sandbox-node-cycle-1-1:8090")
        self.assertEqual(intents[0].options.name, "ucloud-sandbox-node-cycle-1-1")
        item = intents[0].options.job_item()
        self.assertEqual(item["resources"], [{"type": "private_network", "id": "net-1"}])
        self.assertIsNone(intents[0].options.public_link_id)
        self.assertFalse(item["sshEnabled"])
        self.assertEqual(item["labels"]["ucloud-sandboxes/reconcile"], "true")
        self.assertEqual(item["labels"]["ucloud-sandboxes/reconcile-cycle"], "cycle-1")
        self.assertEqual(item["labels"]["ucloud-sandboxes/deployment"], "prod-a")
        self.assertIn("ucloud-sandboxes/agent-version", item["labels"])
        self.assertIn("ucloud-sandboxes/init-version", item["labels"])

    def test_builds_bulk_payload_for_create_intents(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            private_network_id="net-1",
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        decision = evaluate_scale(
            [],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=4, memory_mb=12_288)),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )
        intents = build_vm_create_intents(
            config,
            decision,
            VmNodeSubmissionDefaults(private_network_id=config.private_network_id),
            seed_prefix="cycle-1",
        )

        payload = bulk_payload_from_create_intents(intents)

        self.assertEqual(payload["type"], "bulk")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["hostname"], "sandbox-node-cycle-1-1")

    def test_extracts_stop_job_ids_from_decision(self) -> None:
        decision = ScaleDecision(
            actions=(
                ScaleAction(kind="stop", count=2, job_ids=("job-1", "job-2")),
            ),
            ready_nodes=2,
            provisioning_nodes=0,
            total_nodes=2,
            reasons=("idle",),
            pending_resources=ResourceQuantity(),
            desired_resources=ResourceQuantity(),
            projected_free_resources=ResourceQuantity(),
            resource_deficit=ResourceQuantity(),
        )

        self.assertEqual(stop_job_ids_from_decision(decision), ("job-1", "job-2"))

    def test_partitions_stop_job_ids_by_deployment_label(self) -> None:
        class Node:
            def __init__(self, job):
                self.job = job

        owned = VmJob(
            id="job-1",
            project_id="project-1",
            name="ucloud-sandbox-node-1",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="RUNNING",
            labels={
                "ucloud-sandboxes/node": "true",
                "ucloud-sandboxes/deployment": "prod-a",
            },
        )
        foreign = VmJob(
            id="job-2",
            project_id="project-1",
            name="ucloud-sandbox-node-2",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="RUNNING",
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
