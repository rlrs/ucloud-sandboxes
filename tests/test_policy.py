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


_UNSET = object()


def node(
    job_id: str,
    *,
    state: str = "RUNNING",
    active: int = 0,
    active_image_builds: int = 0,
    fresh: bool = True,
    created_at=_UNSET,
    started_at=None,
    total_resources: ResourceQuantity | None = None,
    used_resources: ResourceQuantity | None = None,
    cpu_overcommit: float = 1.0,
    memory_overcommit: float = 1.0,
    capabilities: tuple[str, ...] = ("disk-quota",),
    idle_since=None,
    heartbeat_updated_at=None,
    heartbeat_present: bool = True,
    inventory_complete: bool = False,
    agent_version_compatible: bool = True,
    draining: bool = False,
    admission_open: bool = True,
    job_cpu: int = 2,
    job_memory_gb: int = 6,
    job_disk_gb: int | None = None,
) -> SandboxNode:
    heartbeat = None
    if heartbeat_present:
        heartbeat = NodeHeartbeat(
            node_id=f"node-{job_id}",
            job_id=job_id,
            updated_at=heartbeat_updated_at or utc_now() - timedelta(seconds=5),
            active_sandboxes=active,
            active_image_builds=active_image_builds,
            idle_since=idle_since,
            total_resources=total_resources or ResourceQuantity(),
            used_resources=used_resources or ResourceQuantity(),
            cpu_overcommit=cpu_overcommit,
            memory_overcommit=memory_overcommit,
            capabilities=capabilities,
            draining=draining,
            admission_open=admission_open,
            inventory_complete=inventory_complete,
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
            created_at=utc_now() if created_at is _UNSET else created_at,
            started_at=started_at,
            cpu=job_cpu,
            memory_gb=job_memory_gb,
            disk_gb=job_disk_gb,
        ),
        heartbeat=heartbeat,
        active_sandboxes=active,
        heartbeat_fresh=fresh,
        agent_version_compatible=agent_version_compatible,
    )


class ScalePolicyTests(unittest.TestCase):
    def test_draining_or_admission_closed_node_contributes_no_ready_capacity(
        self,
    ) -> None:
        for kwargs in ({"draining": True}, {"admission_open": False}):
            with self.subTest(**kwargs):
                decision = evaluate_scale(
                    [
                        node(
                            "draining",
                            total_resources=ResourceQuantity(
                                vcpu=4, memory_mb=8192, disk_mb=10000
                            ),
                            **kwargs,
                        )
                    ],
                    SandboxDemand(
                        pending_resources=ResourceQuantity(
                            vcpu=1, memory_mb=1024, disk_mb=1000
                        )
                    ),
                    ScalePolicy(max_nodes=2, max_create_per_cycle=1),
                )

                self.assertEqual(decision.ready_nodes, 0)
                self.assertEqual(decision.projected_free_resources, ResourceQuantity())
                self.assertEqual(decision.creates, 1)

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

    def test_default_policy_bursts_large_backlog_without_waiting_for_one_node(
        self,
    ) -> None:
        decision = evaluate_scale(
            [
                node(
                    "ready",
                    total_resources=ResourceQuantity(
                        vcpu=16,
                        memory_mb=32768,
                        disk_mb=204800,
                    ),
                    cpu_overcommit=2.0,
                    memory_overcommit=1.2,
                )
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=256,
                    memory_mb=262144,
                    disk_mb=524288,
                )
            ),
            ScalePolicy(),
        )

        self.assertEqual(decision.creates, 4)
        self.assertEqual(decision.resource_deficit.vcpu, 224)

    def test_create_count_uses_effective_overcommitted_node_capacity(self) -> None:
        decision = evaluate_scale(
            [],
            SandboxDemand(
                prepared_resources=ResourceQuantity(
                    vcpu=100,
                    memory_mb=51200,
                    disk_mb=102400,
                )
            ),
            ScalePolicy(
                max_nodes=10,
                max_create_per_cycle=10,
                cpu_overcommit=2.0,
                memory_overcommit=1.2,
            ),
        )

        self.assertEqual(decision.creates, 2)
        self.assertEqual(
            decision.desired_resources,
            ResourceQuantity(vcpu=100, memory_mb=51200, disk_mb=102400),
        )

    def test_booting_node_estimate_includes_effective_overcommit(self) -> None:
        decision = evaluate_scale(
            [node("booting", state="IN_QUEUE", fresh=False)],
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=4, memory_mb=12000)
            ),
            ScalePolicy(
                max_nodes=5,
                max_create_per_cycle=5,
                cpu_overcommit=2.0,
                memory_overcommit=2.0,
            ),
        )

        self.assertEqual(decision.projected_free_resources.vcpu, 4)
        self.assertEqual(decision.projected_free_resources.memory_mb, 12288)
        self.assertEqual(decision.creates, 0)

    def test_stale_heartbeat_credits_free_not_total_resources(self) -> None:
        decision = evaluate_scale(
            [
                node(
                    "stale",
                    state="RUNNING",
                    fresh=False,
                    total_resources=ResourceQuantity(
                        vcpu=16,
                        memory_mb=32768,
                        disk_mb=204800,
                    ),
                    used_resources=ResourceQuantity(
                        vcpu=32,
                        memory_mb=16384,
                        disk_mb=32768,
                    ),
                    cpu_overcommit=2.0,
                    job_cpu=16,
                    job_memory_gb=32,
                    job_disk_gb=200,
                )
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=1)),
            ScalePolicy(
                max_nodes=2,
                max_create_per_cycle=1,
                cpu_overcommit=2.0,
            ),
        )

        self.assertEqual(decision.projected_free_resources.vcpu, 0)
        self.assertEqual(decision.creates, 1)

    def test_stops_unreachable_empty_node_after_eviction_lease(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "lost",
                    fresh=False,
                    heartbeat_updated_at=now - timedelta(hours=1),
                    inventory_complete=True,
                )
            ],
            SandboxDemand(),
            ScalePolicy(
                max_stop_per_cycle=1,
                unreachable_stop_after_seconds=1800,
            ),
            now=now,
        )

        self.assertEqual(decision.stops, ("lost",))
        self.assertIn("unreachable empty", decision.reasons[0])

    def test_does_not_stop_unreachable_node_without_empty_inventory_proof(
        self,
    ) -> None:
        now = utc_now()
        for candidate in (
            node(
                "incomplete",
                fresh=False,
                heartbeat_updated_at=now - timedelta(hours=1),
                inventory_complete=False,
            ),
            node(
                "routed",
                active=1,
                fresh=False,
                heartbeat_updated_at=now - timedelta(hours=1),
                inventory_complete=True,
            ),
        ):
            with self.subTest(job_id=candidate.job_id):
                decision = evaluate_scale(
                    [candidate],
                    SandboxDemand(),
                    ScalePolicy(unreachable_stop_after_seconds=1800),
                    now=now,
                )
                self.assertEqual(decision.stops, ())

    def test_stops_never_ready_vm_after_unreachable_eviction_lease(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "never-ready",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=now - timedelta(hours=1),
                    started_at=now - timedelta(hours=1),
                )
            ],
            SandboxDemand(),
            ScalePolicy(unreachable_stop_after_seconds=1800),
            now=now,
        )

        self.assertEqual(decision.stops, ("never-ready",))

    def test_old_backlog_does_not_age_newly_submitted_capacity(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "new",
                    state="IN_QUEUE",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=now,
                )
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=2),
                oldest_pending_seconds=3600,
            ),
            ScalePolicy(
                max_nodes=2,
                max_create_per_cycle=1,
                stale_provisioning_after_seconds=60,
                stale_provisioning_capacity_weight=0.0,
            ),
            now=now,
        )

        self.assertEqual(decision.projected_free_resources.vcpu, 2)
        self.assertEqual(decision.creates, 0)

    def test_provisioning_disk_estimate_is_capped_to_node_quota(self) -> None:
        decision = evaluate_scale(
            [
                node(
                    "queued",
                    state="IN_QUEUE",
                    fresh=False,
                    heartbeat_present=False,
                    job_cpu=16,
                    job_memory_gb=32,
                    job_disk_gb=250,
                )
            ],
            SandboxDemand(pending_resources=ResourceQuantity(disk_mb=204800)),
            ScalePolicy(max_nodes=2, max_create_per_cycle=1),
        )

        self.assertEqual(decision.projected_free_resources.disk_mb, 204800)
        self.assertEqual(decision.creates, 0)

    def test_staged_preparation_reaches_100_sandboxes_with_four_nodes(self) -> None:
        policy = ScalePolicy(
            max_nodes=10,
            max_create_per_cycle=10,
            cpu_overcommit=2.0,
            memory_overcommit=1.2,
        )
        ready_empty = [
            node(
                str(index),
                total_resources=ResourceQuantity(
                    vcpu=16,
                    memory_mb=32768,
                    disk_mb=204800,
                ),
                cpu_overcommit=2.0,
                memory_overcommit=1.2,
                job_cpu=16,
                job_memory_gb=32,
                job_disk_gb=200,
            )
            for index in range(2)
        ]
        first = evaluate_scale(
            ready_empty,
            SandboxDemand(prepared_resources=ResourceQuantity(vcpu=67)),
            policy,
        )
        booting = node(
            "booting-3",
            state="IN_QUEUE",
            fresh=False,
            heartbeat_present=False,
            job_cpu=16,
            job_memory_gb=32,
            job_disk_gb=200,
        )
        second = evaluate_scale(
            [*ready_empty, booting],
            SandboxDemand(prepared_resources=ResourceQuantity(vcpu=33)),
            policy,
        )
        ready_full = [
            node(
                str(index),
                active=32,
                total_resources=ResourceQuantity(
                    vcpu=16,
                    memory_mb=32768,
                    disk_mb=204800,
                ),
                used_resources=ResourceQuantity(vcpu=32),
                cpu_overcommit=2.0,
                memory_overcommit=1.2,
                job_cpu=16,
                job_memory_gb=32,
                job_disk_gb=200,
            )
            for index in range(2)
        ]
        final = evaluate_scale(
            [*ready_full, booting],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=36)),
            policy,
        )

        self.assertEqual(first.creates, 1)
        self.assertEqual(second.creates, 0)
        self.assertEqual(second.resource_deficit, ResourceQuantity())
        self.assertEqual(final.creates, 1)

    def test_stops_idle_incompatible_node_without_idle_grace(self) -> None:
        decision = evaluate_scale(
            [
                node(
                    "old-idle",
                    agent_version_compatible=False,
                )
            ],
            SandboxDemand(),
            ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=600),
        )

        self.assertEqual(decision.stops, ("old-idle",))
        self.assertEqual(decision.total_nodes, 1)
        self.assertIn("incompatible agent version", decision.reasons[0])

    def test_active_incompatible_node_is_not_stopped_or_counted_as_capacity(
        self,
    ) -> None:
        decision = evaluate_scale(
            [
                node(
                    "old-active",
                    active=1,
                    agent_version_compatible=False,
                )
            ],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=1, memory_mb=512)),
            ScalePolicy(max_nodes=5, max_create_per_cycle=5, max_stop_per_cycle=5),
        )

        self.assertEqual(decision.stops, ())
        self.assertEqual(decision.creates, 1)
        self.assertEqual(decision.total_nodes, 1)
        self.assertEqual(decision.projected_free_resources, ResourceQuantity())

    def test_active_incompatible_node_still_blocks_hard_max_nodes(self) -> None:
        decision = evaluate_scale(
            [node("old-active", active=1, agent_version_compatible=False)],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=1, memory_mb=512)),
            ScalePolicy(max_nodes=1, max_create_per_cycle=5),
        )

        self.assertEqual(decision.total_nodes, 1)
        self.assertEqual(decision.projected_free_resources, ResourceQuantity())
        self.assertEqual(decision.creates, 0)
        self.assertIn("max_nodes=1 reached", decision.reasons[0])

    def test_booting_unversioned_node_prevents_replacement_stampede(self) -> None:
        now = utc_now()
        decision = evaluate_scale(
            [
                node(
                    "booting",
                    state="RUNNING",
                    fresh=False,
                    heartbeat_present=False,
                    agent_version_compatible=False,
                    started_at=now - timedelta(seconds=30),
                )
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(
                    vcpu=1,
                    memory_mb=1024,
                    disk_mb=1024,
                )
            ),
            ScalePolicy(max_nodes=8, max_create_per_cycle=4),
            now=now,
        )

        self.assertEqual(decision.provisioning_nodes, 1)
        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.projected_free_resources.vcpu, 2)
        self.assertGreaterEqual(decision.projected_free_resources.disk_mb, 1024)

    def test_respects_max_nodes_for_resource_deficit(self) -> None:
        decision = evaluate_scale(
            [
                node("1", total_resources=ResourceQuantity(vcpu=2, memory_mb=6144)),
                node("2", total_resources=ResourceQuantity(vcpu=2, memory_mb=6144)),
            ],
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=10, memory_mb=30_000)
            ),
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

    def test_counts_queued_vm_capacity_fully_by_default(self) -> None:
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

    def test_stale_suspended_vm_has_no_capacity_but_counts_toward_limits(self) -> None:
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

        self.assertEqual(decision.total_nodes, 1)
        self.assertEqual(decision.provisioning_nodes, 1)
        self.assertEqual(decision.projected_free_resources, ResourceQuantity())
        self.assertEqual(decision.creates, 1)

    def test_stale_suspended_vm_blocks_hard_node_and_provisioning_limits(self) -> None:
        now = utc_now()
        stale = node(
            "submitted",
            state="SUSPENDED",
            fresh=False,
            heartbeat_present=False,
            created_at=now - timedelta(seconds=3600),
        )
        policy = ScalePolicy(
            max_nodes=1,
            max_provisioning_nodes=1,
            max_create_per_cycle=5,
            stale_provisioning_after_seconds=60,
            stale_provisioning_capacity_weight=0.0,
        )

        decision = evaluate_scale(
            [stale],
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=2, memory_mb=4096)),
            policy,
            now=now,
        )

        self.assertEqual(decision.total_nodes, 1)
        self.assertEqual(decision.provisioning_nodes, 1)
        self.assertEqual(decision.projected_free_resources, ResourceQuantity())
        self.assertEqual(decision.creates, 0)
        self.assertTrue(
            any(
                "max_nodes=1 reached" in reason
                or "max_provisioning_nodes=1 reached" in reason
                for reason in decision.reasons
            )
        )

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
            SandboxDemand(pending_resources=ResourceQuantity(vcpu=2, memory_mb=6144)),
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
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=10, memory_mb=30_000)
            ),
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
            SandboxDemand(
                pending_resources=ResourceQuantity(vcpu=10, memory_mb=30_000)
            ),
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

        self.assertEqual(decision.total_nodes, 1)
        self.assertEqual(decision.provisioning_nodes, 1)
        self.assertEqual(decision.projected_free_resources, ResourceQuantity())
        self.assertEqual(decision.creates, 1)

    def test_running_provisioning_uses_created_time_when_start_time_is_missing(
        self,
    ) -> None:
        now = utc_now()
        policy = ScalePolicy(
            max_nodes=5,
            max_create_per_cycle=5,
            stale_provisioning_after_seconds=60,
        )
        demand = SandboxDemand(
            pending_resources=ResourceQuantity(vcpu=2, memory_mb=4096)
        )

        old_created = evaluate_scale(
            [
                node(
                    "running-no-start",
                    state="RUNNING",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=now - timedelta(seconds=3600),
                )
            ],
            demand,
            policy,
            now=now,
        )
        unknown_age = evaluate_scale(
            [
                node(
                    "running-no-time",
                    state="RUNNING",
                    fresh=False,
                    heartbeat_present=False,
                    created_at=None,
                )
            ],
            demand,
            policy,
            now=now,
        )

        self.assertEqual(old_created.projected_free_resources, ResourceQuantity())
        self.assertEqual(unknown_age.projected_free_resources, ResourceQuantity())

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
                prepared_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=2048
                )
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
                pending_resources=ResourceQuantity(
                    vcpu=1, memory_mb=1024, disk_mb=2048
                ),
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
            policy=ScalePolicy(
                max_stop_per_cycle=1, builder_scale_down_idle_seconds=60
            ),
            max_builder_nodes=1,
            now=now,
        )

        self.assertEqual(decision.stops, ())
        self.assertIn("builder pool matches demand", decision.reasons[0])


if __name__ == "__main__":
    unittest.main()
