from dataclasses import replace
from datetime import timedelta
import unittest

from ucloud_sandboxes.models import (
    ResourceQuantity,
    SandboxDemand,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.policy import evaluate_scale
from ucloud_sandboxes.program_scheduler import build_program_scale_signals
from ucloud_sandboxes.program_scheduler import (
    WakeNodeCandidate,
    plan_shadow_wake_queue,
)
from ucloud_sandboxes.routing import ProgramRequestState, SandboxRoute


class ProgramSchedulerTests(unittest.TestCase):
    def test_shadow_wake_queue_ages_first_and_prefers_local_hard_fit(self) -> None:
        now = utc_now()
        routes = [
            SandboxRoute(
                sandbox_id=f"sandbox-{index}",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                generation=1,
                state="parked",
                resources=ResourceQuantity(
                    vcpu=2,
                    memory_mb=2048,
                    disk_mb=8000,
                ),
            )
            for index in range(2)
        ]
        requests = [
            ProgramRequestState(
                request_id=f"request-{index}",
                rollout_id=f"rollout-{index}",
                sandbox_id=route.sandbox_id,
                sandbox_generation=route.generation,
                state="ready_to_wake",
                resources=route.resources,
                response_ready_at=(
                    now - timedelta(seconds=20 - index * 15)
                ).isoformat(),
            )
            for index, route in enumerate(routes)
        ]
        candidates = [
            WakeNodeCandidate(
                node_id="node-1",
                job_id="job-1",
                available=ResourceQuantity(
                    vcpu=4,
                    memory_mb=4096,
                    disk_mb=0,
                ),
                total=ResourceQuantity(
                    vcpu=8,
                    memory_mb=8192,
                    disk_mb=100000,
                ),
                pressure=0.5,
            ),
            WakeNodeCandidate(
                node_id="node-2",
                job_id="job-2",
                available=ResourceQuantity(
                    vcpu=8,
                    memory_mb=8192,
                    disk_mb=100000,
                ),
                total=ResourceQuantity(
                    vcpu=8,
                    memory_mb=8192,
                    disk_mb=100000,
                ),
            ),
        ]

        plan = plan_shadow_wake_queue(
            requests,
            routes,
            candidates,
            now=now,
        )

        placements = plan["placements"]
        assert isinstance(placements, list)
        self.assertEqual(placements[0]["request_id"], "request-0")
        self.assertEqual(placements[0]["node_id"], "node-1")
        self.assertTrue(placements[0]["local"])
        self.assertEqual(plan["unplaced_count"], 0)

    def test_shadow_wake_queue_reports_unplaced_hard_shape(self) -> None:
        route = SandboxRoute(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            generation=1,
            state="parked",
            resources=ResourceQuantity(vcpu=16, memory_mb=32768, disk_mb=50000),
        )
        request = ProgramRequestState(
            request_id="request-1",
            rollout_id="rollout-1",
            sandbox_id=route.sandbox_id,
            sandbox_generation=route.generation,
            state="ready_to_wake",
            resources=route.resources,
        )

        plan = plan_shadow_wake_queue(
            [request],
            [route],
            [
                WakeNodeCandidate(
                    node_id="node-2",
                    job_id="job-2",
                    available=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16384,
                        disk_mb=100000,
                    ),
                    total=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16384,
                        disk_mb=100000,
                    ),
                )
            ],
        )

        self.assertEqual(plan["placed"], 0)
        self.assertEqual(plan["unplaced_count"], 1)
        unplaced = plan["unplaced"]
        assert isinstance(unplaced, list)
        self.assertEqual(unplaced[0]["reason"], "no_hard_fit")

    def test_program_demand_deduplicates_concurrent_requests_per_sandbox(self) -> None:
        now = utc_now()
        route = SandboxRoute(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            generation=2,
            state="parked",
            resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=32768),
        )
        requests = [
            ProgramRequestState(
                request_id="wait",
                rollout_id="rollout-1",
                sandbox_id=route.sandbox_id,
                sandbox_generation=route.generation,
                state="model_wait",
                resources=route.resources,
                accepted_at=(now - timedelta(seconds=30)).isoformat(),
            ),
            ProgramRequestState(
                request_id="ready",
                rollout_id="rollout-1",
                sandbox_id=route.sandbox_id,
                sandbox_generation=route.generation,
                state="ready_to_wake",
                resources=route.resources,
                response_ready_at=(now - timedelta(seconds=10)).isoformat(),
            ),
        ]

        signals = build_program_scale_signals(
            requests,
            [route],
            ScalePolicy(model_wait_capacity_weight=0.5),
            now=now,
        )

        self.assertEqual(signals.model_wait_requests, 1)
        self.assertEqual(signals.ready_to_wake_requests, 1)
        self.assertEqual(signals.model_wait_sandboxes, 0)
        self.assertEqual(signals.ready_to_wake_sandboxes, 1)
        self.assertEqual(
            signals.ready_to_wake_resources,
            ResourceQuantity(vcpu=4, memory_mb=8192),
        )
        self.assertEqual(len(signals.ready_placement_requests), 1)
        self.assertEqual(
            signals.ready_placement_requests[0].resources,
            route.resources,
        )
        self.assertEqual(
            signals.ready_placement_requests[0].owned_job_id,
            route.job_id,
        )
        self.assertEqual(
            signals.ready_placement_requests[0].owned_disk_mb,
            route.resources.disk_mb,
        )

    def test_completed_acting_request_does_not_hide_new_model_wait(self) -> None:
        route = SandboxRoute(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            generation=1,
            state="parked",
            resources=ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=8192),
        )
        requests = [
            ProgramRequestState(
                request_id="previous",
                rollout_id="rollout-1",
                sandbox_id=route.sandbox_id,
                sandbox_generation=route.generation,
                state="acting",
                resources=route.resources,
            ),
            ProgramRequestState(
                request_id="current",
                rollout_id="rollout-1",
                sandbox_id=route.sandbox_id,
                sandbox_generation=route.generation,
                state="model_wait",
                resources=route.resources,
            ),
        ]

        signals = build_program_scale_signals(
            requests,
            [route],
            ScalePolicy(model_wait_capacity_weight=0.5),
        )

        self.assertEqual(signals.acting_requests, 1)
        self.assertEqual(signals.model_wait_sandboxes, 1)
        self.assertEqual(
            signals.weighted_model_wait_resources,
            ResourceQuantity(vcpu=1, memory_mb=2048),
        )

    def test_model_wait_demand_is_weighted_capped_and_shadow_only_by_default(
        self,
    ) -> None:
        policy = ScalePolicy(
            model_wait_capacity_weight=0.5,
            model_wait_max_headroom_nodes=1,
            default_node_resources=ResourceQuantity(
                vcpu=8,
                memory_mb=16384,
                disk_mb=100000,
            ),
        )
        routes = []
        requests = []
        for index in range(4):
            route = SandboxRoute(
                sandbox_id=f"sandbox-{index}",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                generation=1,
                state="parked",
                resources=ResourceQuantity(
                    vcpu=8,
                    memory_mb=16384,
                    disk_mb=10000,
                ),
            )
            routes.append(route)
            requests.append(
                ProgramRequestState(
                    request_id=f"request-{index}",
                    rollout_id=f"rollout-{index}",
                    sandbox_id=route.sandbox_id,
                    sandbox_generation=route.generation,
                    state="model_wait",
                    resources=route.resources,
                )
            )

        shadow = build_program_scale_signals(requests, routes, policy)
        enabled = build_program_scale_signals(
            requests,
            routes,
            replace(policy, program_aware_autoscaling_enabled=True),
        )

        self.assertEqual(
            shadow.weighted_model_wait_resources,
            ResourceQuantity(vcpu=8, memory_mb=16384),
        )
        self.assertEqual(shadow.effective_resources, ResourceQuantity())
        self.assertEqual(
            enabled.effective_resources,
            ResourceQuantity(vcpu=8, memory_mb=16384),
        )

    def test_pending_wake_is_not_counted_twice(self) -> None:
        route = SandboxRoute(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            generation=1,
            state="parked",
            resources=ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=8192),
        )
        request = ProgramRequestState(
            request_id="request-1",
            rollout_id="rollout-1",
            sandbox_id=route.sandbox_id,
            sandbox_generation=route.generation,
            state="ready_to_wake",
            resources=route.resources,
        )

        signals = build_program_scale_signals(
            [request],
            [route],
            ScalePolicy(program_aware_autoscaling_enabled=True),
            pending_wake_sandbox_ids={"sandbox-1"},
        )

        self.assertEqual(signals.ready_to_wake_sandboxes, 0)
        self.assertEqual(signals.effective_resources, ResourceQuantity())
        self.assertEqual(signals.ready_placement_requests, ())

    def test_enabled_program_demand_participates_in_scale_decision(self) -> None:
        policy = ScalePolicy(
            program_aware_autoscaling_enabled=True,
            default_node_resources=ResourceQuantity(
                vcpu=8,
                memory_mb=16384,
                disk_mb=100000,
            ),
        )
        route = SandboxRoute(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            generation=1,
            state="parked",
            resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=10000),
        )
        request = ProgramRequestState(
            request_id="request-1",
            rollout_id="rollout-1",
            sandbox_id=route.sandbox_id,
            sandbox_generation=route.generation,
            state="ready_to_wake",
            resources=route.resources,
        )
        signals = build_program_scale_signals([request], [route], policy)

        decision = evaluate_scale(
            [],
            SandboxDemand(),
            policy,
            program_signals=signals,
        )

        self.assertEqual(decision.creates, 1)
        self.assertEqual(decision.desired_resources.vcpu, 4)
        self.assertEqual(decision.desired_resources.memory_mb, 8192)
        self.assertEqual(decision.desired_resources.disk_mb, 0)


if __name__ == "__main__":
    unittest.main()
