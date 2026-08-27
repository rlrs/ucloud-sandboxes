from dataclasses import replace
from datetime import timedelta
import unittest

from hypothesis import given, strategies as st

from ucloud_sandboxes.models import (
    ResourceQuantity,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.program_scheduler import build_program_scale_signals
from ucloud_sandboxes.program_scheduler import (
    WakeNodeCandidate,
    plan_shadow_wake_queue,
)
from ucloud_sandboxes.routing import ProgramRequestState, SandboxRoute


def sandbox_route(**values: object) -> SandboxRoute:
    values.setdefault("resources", ResourceQuantity())
    values.setdefault("spec", {"id": values.get("sandbox_id")})
    values.setdefault("state", "unknown")
    values.setdefault("generation", 1)
    values.setdefault("create_operation_id", "create-test-route")
    values.setdefault("spec_hash", "a" * 64)
    return SandboxRoute(**values)  # type: ignore[arg-type]


class ProgramSchedulerTests(unittest.TestCase):
    def test_nonportable_park_can_only_plan_on_its_current_worker(self) -> None:
        route = sandbox_route(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            state="parked",
            resources=ResourceQuantity(vcpu=2, memory_mb=2048, disk_mb=8000),
        )
        request = ProgramRequestState(
            request_id="request-1",
            rollout_id="rollout-1",
            sandbox_id=route.sandbox_id,
            sandbox_generation=route.generation,
            state="ready_to_wake",
            resources=route.resources,
        )
        remote_only = [
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
        ]

        plan = plan_shadow_wake_queue([request], [route], remote_only)
        signals = build_program_scale_signals(
            [request],
            [route],
            ScalePolicy(program_aware_autoscaling_enabled=True),
        )

        self.assertEqual(plan["placed"], 0)
        self.assertEqual(plan["unplaced"][0]["reason"], "route_not_portable")
        self.assertEqual(signals.ready_to_wake_requests, 1)
        self.assertEqual(signals.ready_to_wake_sandboxes, 0)
        self.assertEqual(signals.effective_resources, ResourceQuantity())

    def test_shadow_wake_queue_ages_first_and_prefers_local_hard_fit(self) -> None:
        now = utc_now()
        routes = [
            sandbox_route(
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
        hard_route = sandbox_route(
            sandbox_id="sandbox-hard",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            state="parked",
            resources=ResourceQuantity(vcpu=16, memory_mb=32768, disk_mb=50000),
        )
        routes.append(hard_route)
        requests.append(
            ProgramRequestState(
                request_id="request-hard",
                rollout_id="rollout-hard",
                sandbox_id=hard_route.sandbox_id,
                sandbox_generation=hard_route.generation,
                state="ready_to_wake",
                resources=hard_route.resources,
            )
        )
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
        self.assertEqual(plan["unplaced_count"], 1)
        self.assertEqual(plan["unplaced"][0]["reason"], "route_not_portable")

    @given(
        states=st.lists(
            st.sampled_from(("model_wait", "ready_to_wake", "waking", "acting")),
            max_size=6,
        ),
        pending=st.booleans(),
        enabled=st.booleans(),
    )
    def test_program_demand_reduces_each_sandbox_to_one_future_phase(
        self,
        states: list[str],
        pending: bool,
        enabled: bool,
    ) -> None:
        route = sandbox_route(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            generation=1,
            state="parked",
            resources=ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=8192),
            storage_schema="storage-native-v1",
            snapshot_manifest_digest="sha256:" + "d" * 64,
            snapshot_repository="sandboxes/snapshots",
            snapshot_tag="sandbox-1-g1",
            storage_snapshot={"published": True},
        )
        requests = [
            ProgramRequestState(
                request_id=f"request-{index}",
                rollout_id=f"rollout-{index}",
                sandbox_id=route.sandbox_id,
                sandbox_generation=route.generation,
                state=state,
                resources=route.resources,
            )
            for index, state in enumerate(states)
        ]
        policy = ScalePolicy(
            program_aware_autoscaling_enabled=enabled,
            model_wait_capacity_weight=1,
            model_wait_max_headroom_nodes=10,
            default_node_resources=ResourceQuantity(vcpu=8, memory_mb=16384),
        )
        signals = build_program_scale_signals(
            requests,
            [route],
            policy,
            pending_wake_sandbox_ids={"sandbox-1"} if pending else set(),
        )
        for state in ("model_wait", "ready_to_wake", "waking", "acting"):
            self.assertEqual(
                getattr(signals, f"{state}_requests"),
                states.count(state),
            )
        ready = "ready_to_wake" in states and not pending
        waiting = "model_wait" in states and "ready_to_wake" not in states
        self.assertEqual(signals.ready_to_wake_sandboxes, int(ready))
        self.assertEqual(signals.model_wait_sandboxes, int(waiting))
        expected = (
            ResourceQuantity(vcpu=2, memory_mb=4096)
            if ready or waiting
            else ResourceQuantity()
        )
        self.assertEqual(
            signals.effective_resources,
            expected if enabled else ResourceQuantity(),
        )
        self.assertEqual(len(signals.ready_placement_requests), int(ready))

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
            route = sandbox_route(
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


if __name__ == "__main__":
    unittest.main()
