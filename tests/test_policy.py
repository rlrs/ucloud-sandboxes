from datetime import timedelta
import unittest

from ucloud_sandboxes.models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxDemand,
    SandboxNode,
    ScalePolicy,
    VmJob,
    utc_now,
)
from ucloud_sandboxes.policy import evaluate_scale
from ucloud_sandboxes.reconcile import evaluate_builder_scale


def node(
    job_id: str,
    *,
    state: str = "RUNNING",
    active: int = 0,
    active_image_builds: int = 0,
    fresh: bool = True,
    created_at=None,
    started_at=None,
    total_resources: ResourceQuantity | None = None,
    used_resources: ResourceQuantity | None = None,
    cpu_overcommit: float = 1.0,
    memory_overcommit: float = 1.0,
    capabilities: tuple[str, ...] = ("disk-quota",),
    idle_since=None,
    heartbeat_present: bool = True,
) -> SandboxNode:
    heartbeat = None
    if heartbeat_present:
        heartbeat = NodeHeartbeat(
            node_id=f"node-{job_id}",
            job_id=job_id,
            updated_at=utc_now() - timedelta(seconds=5),
            active_sandboxes=active,
            active_image_builds=active_image_builds,
            idle_since=idle_since,
            total_resources=total_resources or ResourceQuantity(),
            used_resources=used_resources or ResourceQuantity(),
            cpu_overcommit=cpu_overcommit,
            memory_overcommit=memory_overcommit,
            capabilities=capabilities,
        )
    return SandboxNode(
        job=VmJob(
            id=job_id,
            project_id="project-1",
            name=f"ucloud-sandbox-node-{job_id}",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state=state,
            created_at=created_at,
            started_at=started_at,
            cpu=2,
            memory_gb=6,
        ),
        heartbeat=heartbeat,
        active_sandboxes=active,
        heartbeat_fresh=fresh,
    )


class ScalePolicyTests(unittest.TestCase):
    def test_creates_for_resource_deficit(self) -> None:
        decision = evaluate_scale(
            [],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=3.0,
                    memory_mb=7000,
                    disk_mb=0,
                )
            ),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )

        self.assertEqual(decision.creates, 1)
        self.assertEqual(decision.resource_deficit.vcpu, 3.0)
        self.assertEqual(decision.resource_deficit.memory_mb, 7000)

    def test_respects_max_nodes_for_resource_deficit(self) -> None:
        decision = evaluate_scale(
            [
                node("1", total_resources=ResourceQuantity(vcpu=2, memory_mb=6144)),
                node("2", total_resources=ResourceQuantity(vcpu=2, memory_mb=6144)),
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=10, memory_mb=30_000)),
            ScalePolicy(max_nodes=2, max_create_per_cycle=5),
        )

        self.assertEqual(decision.creates, 0)
        self.assertIn("max_nodes=2 reached", decision.reasons[0])

    def test_does_not_create_when_ready_resources_fit(self) -> None:
        decision = evaluate_scale(
            [
                node(
                    "1",
                    total_resources=ResourceQuantity(
                        vcpu=4,
                        memory_mb=8192,
                        disk_mb=100_000,
                    ),
                    used_resources=ResourceQuantity(
                        vcpu=1,
                        memory_mb=1024,
                        disk_mb=10_000,
                    ),
                    cpu_overcommit=2.0,
                )
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=6,
                    memory_mb=4096,
                    disk_mb=20_000,
                )
            ),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )

        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.projected_free_resources.vcpu, 7)

    def test_disk_demand_ignores_nodes_without_disk_quota_capability(self) -> None:
        decision = evaluate_scale(
            [
                node(
                    "1",
                    total_resources=ResourceQuantity(
                        vcpu=4,
                        memory_mb=8192,
                        disk_mb=100_000,
                    ),
                    capabilities=(),
                )
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=1,
                    memory_mb=1024,
                    disk_mb=20_000,
                )
            ),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )

        self.assertEqual(decision.projected_free_resources.disk_mb, 0)
        self.assertEqual(decision.resource_deficit.disk_mb, 20_000)
        self.assertEqual(decision.creates, 1)

    def test_counts_queued_vm_estimated_resources(self) -> None:
        decision = evaluate_scale(
            [node("queued", state="IN_QUEUE", fresh=False)],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=2,
                    memory_mb=4096,
                    disk_mb=0,
                )
            ),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )

        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.projected_free_resources.vcpu, 2)
        self.assertEqual(decision.projected_free_resources.memory_mb, 6144)

    def test_counts_recent_suspended_vm_as_provisioning_capacity(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "submitted",
                    state="SUSPENDED",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=now - timedelta(seconds=30),
                )
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=1,
                    memory_mb=256,
                    disk_mb=512,
                )
            ),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
            now=now,
        )

        self.assertEqual(decision.provisioning_nodes, 1)
        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.projected_free_resources.vcpu, 2)
        self.assertEqual(decision.projected_free_resources.disk_mb, 204800)

    def test_stale_suspended_vm_is_not_pool_capacity(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "submitted",
                    state="SUSPENDED",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=now - timedelta(seconds=3600),
                )
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=1,
                    memory_mb=256,
                    disk_mb=512,
                )
            ),
            ScalePolicy(
                max_nodes=5,
                max_create_per_cycle=5,
                stale_provisioning_after_seconds=60,
                stale_provisioning_capacity_weight=0.0,
            ),
            now=now,
        )

        self.assertEqual(decision.total_nodes, 0)
        self.assertEqual(decision.provisioning_nodes, 0)
        self.assertEqual(decision.projected_free_resources, ResourceQuantity())
        self.assertEqual(decision.creates, 1)

    def test_counts_provisioning_disk_before_first_heartbeat(self) -> None:
        decision = evaluate_scale(
            [node("queued", state="IN_QUEUE", fresh=False, heartbeat_present=False)],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=1,
                    memory_mb=512,
                    disk_mb=1024,
                )
            ),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )

        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.projected_free_resources.disk_mb, 204800)

    def test_discounted_provisioning_resources_can_create_another_vm(self) -> None:
        decision = evaluate_scale(
            [node("queued", state="IN_QUEUE", fresh=False)],
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=2, memory_mb=6144)
            ),
            ScalePolicy(
                max_nodes=5,
                max_create_per_cycle=5,
                max_provisioning_nodes=2,
                provisioning_capacity_weight=0.5,
            ),
        )

        self.assertEqual(decision.projected_free_resources.vcpu, 1)
        self.assertEqual(decision.projected_free_resources.memory_mb, 3072)
        self.assertEqual(decision.creates, 1)

    def test_max_provisioning_nodes_blocks_stampede(self) -> None:
        decision = evaluate_scale(
            [
                node("queued-1", state="IN_QUEUE", fresh=False),
                node("queued-2", state="IN_QUEUE", fresh=False),
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=10, memory_mb=30_000)),
            ScalePolicy(
                max_nodes=10,
                max_create_per_cycle=5,
                max_provisioning_nodes=2,
                provisioning_capacity_weight=0.5,
            ),
        )

        self.assertEqual(decision.creates, 0)
        self.assertIn("max_provisioning_nodes=2 reached", decision.reasons[0])

    def test_recent_suspended_nodes_count_against_provisioning_limit(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "submitted-1",
                    state="SUSPENDED",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=now - timedelta(seconds=30),
                ),
                node(
                    "submitted-2",
                    state="SUSPENDED",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=now - timedelta(seconds=30),
                ),
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=10, memory_mb=30_000)),
            ScalePolicy(
                max_nodes=10,
                max_create_per_cycle=5,
                max_provisioning_nodes=2,
            ),
            now=now,
        )

        self.assertEqual(decision.creates, 0)
        self.assertIn("max_provisioning_nodes=2 reached", decision.reasons[0])

    def test_stale_provisioning_can_use_lower_resource_credit(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "queued",
                    state="IN_QUEUE",
                    fresh=False,
                    created_at=now - timedelta(seconds=3600),
                )
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=2, memory_mb=4096)),
            ScalePolicy(
                max_nodes=5,
                max_create_per_cycle=5,
                max_provisioning_nodes=2,
                provisioning_capacity_weight=1.0,
                stale_provisioning_after_seconds=60,
                stale_provisioning_capacity_weight=0.25,
            ),
            now=now,
        )

        self.assertEqual(decision.projected_free_resources.vcpu, 0.5)
        self.assertEqual(decision.creates, 1)

    def test_default_stale_provisioning_has_no_resource_credit(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "queued",
                    state="IN_QUEUE",
                    fresh=False,
                    created_at=now - timedelta(seconds=3600),
                )
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=2, memory_mb=4096)),
            ScalePolicy(
                max_nodes=5,
                max_create_per_cycle=5,
                stale_provisioning_after_seconds=60,
            ),
            now=now,
        )

        self.assertEqual(decision.total_nodes, 0)
        self.assertEqual(decision.provisioning_nodes, 0)
        self.assertEqual(decision.projected_free_resources, ResourceQuantity())
        self.assertEqual(decision.creates, 1)

    def test_old_pending_backlog_discounts_provisioning_resources(self) -> None:
        decision = evaluate_scale(
            [node("queued", state="IN_QUEUE", fresh=False)],
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=2, memory_mb=4096),
                oldest_pending_seconds=3600,
            ),
            ScalePolicy(
                max_nodes=5,
                max_create_per_cycle=5,
                max_provisioning_nodes=2,
                provisioning_capacity_weight=1.0,
                stale_provisioning_after_seconds=60,
                stale_provisioning_capacity_weight=0.25,
            ),
        )

        self.assertEqual(decision.projected_free_resources.vcpu, 0.5)
        self.assertEqual(decision.creates, 1)

    def test_warm_resources_create_without_pending_demand(self) -> None:
        decision = evaluate_scale(
            [],
            SandboxDemand(),
            ScalePolicy(
                max_nodes=5,
                max_create_per_cycle=5,
                warm_resources=ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=0),
            ),
        )

        self.assertEqual(decision.creates, 1)
        self.assertEqual(decision.desired_resources.vcpu, 2)

    def test_prepared_resources_create_without_pending_sandboxes(self) -> None:
        decision = evaluate_scale(
            [],
            SandboxDemand(
                prepared_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=2048)
            ),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5),
        )

        self.assertEqual(decision.creates, 1)
        self.assertEqual(decision.pending_resources, ResourceQuantity())
        self.assertEqual(decision.prepared_resources.vcpu, 4)
        self.assertEqual(decision.desired_resources.vcpu, 4)

    def test_does_not_stop_when_resource_demand_exists(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "1",
                    total_resources=ResourceQuantity(vcpu=2, memory_mb=6144),
                    idle_since=now - timedelta(seconds=600),
                ),
                node(
                    "2",
                    total_resources=ResourceQuantity(vcpu=2, memory_mb=6144),
                    idle_since=now - timedelta(seconds=600),
                ),
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=3, memory_mb=1024)),
            ScalePolicy(min_nodes=0, max_stop_per_cycle=1),
            now=now,
        )

        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.stops, ())

    def test_stops_surplus_idle_node_when_demand_fits_remaining_capacity(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "1",
                    total_resources=ResourceQuantity(
                        vcpu=16,
                        memory_mb=32768,
                        disk_mb=204800,
                    ),
                    cpu_overcommit=2.0,
                    memory_overcommit=1.2,
                    idle_since=now - timedelta(seconds=600),
                ),
                node(
                    "2",
                    total_resources=ResourceQuantity(
                        vcpu=16,
                        memory_mb=32768,
                        disk_mb=204800,
                    ),
                    cpu_overcommit=2.0,
                    memory_overcommit=1.2,
                    idle_since=now - timedelta(seconds=600),
                ),
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                prepared_resources=ResourceQuantity(
                    vcpu=8,
                    memory_mb=32768,
                    disk_mb=32768,
                ),
            ),
            ScalePolicy(min_nodes=0, max_stop_per_cycle=1),
            now=now,
        )

        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.stops, ("1",))
        self.assertIn("desired demand", decision.reasons[0])

    def test_does_not_stop_when_prepared_resource_demand_exists(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "1",
                    total_resources=ResourceQuantity(vcpu=2, memory_mb=6144),
                    idle_since=now - timedelta(seconds=600),
                )
            ],
            SandboxDemand(prepared_resources=ResourceQuantity(vcpu=1, memory_mb=1024)),
            ScalePolicy(min_nodes=0, max_stop_per_cycle=1),
            now=now,
        )

        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.stops, ())

    def test_warm_resources_prevent_stopping_too_many_resources(self) -> None:
        decision = evaluate_scale(
            [
                node("1", total_resources=ResourceQuantity(vcpu=2, memory_mb=6144)),
                node("2", total_resources=ResourceQuantity(vcpu=2, memory_mb=6144)),
            ],
            SandboxDemand(),
            ScalePolicy(
                min_nodes=1,
                max_stop_per_cycle=1,
                warm_resources=ResourceQuantity(vcpu=3, memory_mb=0, disk_mb=0),
            ),
        )

        self.assertEqual(decision.stops, ())

    def test_scales_to_zero_when_no_demand_or_warm_policy(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "1",
                    total_resources=ResourceQuantity(vcpu=2, memory_mb=6144),
                    idle_since=now - timedelta(seconds=600),
                )
            ],
            SandboxDemand(),
            ScalePolicy(min_nodes=0, max_stop_per_cycle=1),
            now=now,
        )

        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.stops, ("1",))

    def test_scale_down_idle_grace_uses_idle_since_not_vm_start(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "long-lived",
                    total_resources=ResourceQuantity(vcpu=2, memory_mb=6144),
                    started_at=now - timedelta(seconds=3600),
                    idle_since=now - timedelta(seconds=60),
                )
            ],
            SandboxDemand(),
            ScalePolicy(
                min_nodes=0,
                max_stop_per_cycle=1,
                scale_down_idle_seconds=300,
            ),
            now=now,
        )

        self.assertEqual(decision.stops, ())

    def test_scale_down_idle_grace_keeps_recent_node(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "recent",
                    total_resources=ResourceQuantity(vcpu=2, memory_mb=6144),
                    started_at=now - timedelta(seconds=60),
                ),
                node(
                    "busy",
                    active=1,
                    total_resources=ResourceQuantity(vcpu=2, memory_mb=6144),
                    used_resources=ResourceQuantity(vcpu=1, memory_mb=1024),
                ),
            ],
            SandboxDemand(),
            ScalePolicy(
                min_nodes=1,
                max_stop_per_cycle=1,
                scale_down_idle_seconds=300,
            ),
            now=now,
        )

        self.assertEqual(decision.stops, ())

    def test_prepared_builder_count_scales_builder_pool(self) -> None:
        decision = evaluate_builder_scale(
            [],
            pending_builds=0,
            prepared_builders=2,
            policy=ScalePolicy(max_create_per_cycle=2),
            max_builder_nodes=2,
        )

        self.assertEqual(decision.creates, 2)
        self.assertIn("2 prepared builder", decision.reasons[0])

    def test_active_image_build_prevents_builder_scale_down(self) -> None:
        now = utc_now()
        decision = evaluate_builder_scale(
            [
                node(
                    "builder-1",
                    active_image_builds=1,
                    total_resources=ResourceQuantity(vcpu=16, memory_mb=32768),
                    idle_since=now - timedelta(seconds=3600),
                )
            ],
            pending_builds=0,
            prepared_builders=0,
            policy=ScalePolicy(max_stop_per_cycle=1, builder_scale_down_idle_seconds=60),
            max_builder_nodes=1,
            now=now,
        )

        self.assertEqual(decision.stops, ())
        self.assertIn("builder pool matches demand", decision.reasons[0])


if __name__ == "__main__":
    unittest.main()
