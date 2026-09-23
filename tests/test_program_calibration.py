from dataclasses import replace
from datetime import timedelta
import unittest

from ucloud_sandboxes.models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxInventoryEntry,
    SandboxMemoryObservation,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.program_scheduler import (
    build_program_scale_signals,
    observed_memory_mb,
)
from ucloud_sandboxes.routing import ProgramRequestState
from tests.test_program_scheduler import sandbox_route


class ProgramCalibrationTests(unittest.TestCase):
    def setUp(self):
        self.now = utc_now()
        self.shape = ResourceQuantity(vcpu=2, memory_mb=2048, disk_mb=8000)
        self.route = sandbox_route(
            sandbox_id="s",
            node_id="n",
            job_id="j",
            node_url="http://n",
            state="parked",
            resources=self.shape,
            node_epoch="boot",
        )
        self.entry = SandboxInventoryEntry(
            sandbox_id="s",
            generation=1,
            operation_id="create-s",
            spec_hash=self.route.spec_hash,
            state="parked",
            resources=self.shape,
            memory_observation=SandboxMemoryObservation(
                512 * 1024**2, self.now.isoformat()
            ),
        )
        self.heartbeat = NodeHeartbeat(
            node_id="n",
            job_id="j",
            updated_at=self.now,
            node_epoch="boot",
            active_sandboxes=0,
            inventory=(self.entry,),
        )
        self.wait = ProgramRequestState(
            request_id="waiting",
            rollout_id="r",
            sandbox_id="s",
            sandbox_generation=1,
            state="model_wait",
            resources=self.shape,
            accepted_at=(self.now - timedelta(seconds=10)).isoformat(),
        )

    def history(self, durations):
        return [
            replace(
                self.wait,
                request_id=f"history-{i}",
                state="acting",
                accepted_at=(self.now - timedelta(seconds=duration + 1)).isoformat(),
                response_ready_at=(self.now - timedelta(seconds=1)).isoformat(),
            )
            for i, duration in enumerate(durations)
        ]

    def signals(self, *, lead=20, heartbeats=None, history=(), policy=None):
        return build_program_scale_signals(
            [self.wait, *history],
            [self.route],
            policy or ScalePolicy(),
            now=self.now,
            heartbeats=[self.heartbeat] if heartbeats is None else heartbeats,
            provider_ready_seconds=lead,
        )

    def test_observed_memory_and_residual_waits_use_actual_provider_lead(self):
        history = self.history([15, 25, 50, 70])
        short = self.signals(lead=20, history=history)
        longer = self.signals(lead=60, history=history)
        self.assertEqual(
            short.calibration.resources, ResourceQuantity(vcpu=1, memory_mb=256)
        )
        self.assertEqual(
            longer.calibration.resources, ResourceQuantity(vcpu=2, memory_mb=512)
        )
        self.assertEqual(short.calibration.known_wait_sandboxes, 1)
        self.assertEqual(short.calibration.wait_samples, 4)
        self.assertEqual(short.calibration.observed_memory_sandboxes, 1)
        self.assertEqual(short.effective_resources, ResourceQuantity())
        self.assertEqual(short.calibration.to_dict()["cpu_basis"], "declared_limit")

    def test_unknown_evidence_is_full_declared_demand_not_zero_or_fixed_weight(self):
        for lead, history in ((None, self.history([25])), (20, ())):
            with self.subTest(lead=lead):
                result = self.signals(lead=lead, history=history, heartbeats=[])
                self.assertEqual(
                    result.calibration.resources,
                    ResourceQuantity(vcpu=2, memory_mb=2048),
                )
                self.assertEqual(result.calibration.unknown_memory_sandboxes, 1)
                self.assertEqual(result.calibration.unknown_wait_sandboxes, 1)

    def test_shadow_projection_cannot_enable_or_change_existing_actions(self):
        policy = ScalePolicy(program_aware_autoscaling_enabled=True)
        observed = self.signals(policy=policy, lead=60, history=self.history([25]))
        unknown = self.signals(policy=policy, heartbeats=[], lead=None)
        self.assertEqual(observed.effective_resources, unknown.effective_resources)
        self.assertNotEqual(
            observed.calibration.resources, unknown.calibration.resources
        )

    def test_owner_generation_hash_age_and_boot_fence_historical_memory(self):
        invalid = [
            replace(self.heartbeat, node_id="other"),
            replace(self.heartbeat, job_id="other"),
            replace(self.heartbeat, node_epoch="rebooted"),
            replace(self.heartbeat, updated_at=self.now - timedelta(seconds=61)),
            replace(self.heartbeat, inventory=(replace(self.entry, generation=2),)),
            replace(
                self.heartbeat, inventory=(replace(self.entry, spec_hash="b" * 64),)
            ),
            *[
                replace(
                    self.heartbeat,
                    inventory=(
                        replace(
                            self.entry,
                            memory_observation=SandboxMemoryObservation(
                                123, (self.now + timedelta(seconds=delta)).isoformat()
                            ),
                        ),
                    ),
                )
                for delta in (-61, 1)
            ],
        ]
        for heartbeat in invalid:
            with self.subTest(heartbeat=heartbeat):
                self.assertIsNone(
                    observed_memory_mb(
                        self.route, [heartbeat], now=self.now, max_age_seconds=60
                    )
                )

    def test_history_does_not_cross_incarnation_or_count_duplicate_requests(self):
        history = self.history([25])
        result = self.signals(
            history=[
                *history,
                *history,
                replace(history[0], request_id="old", sandbox_generation=2),
            ]
        )
        self.assertEqual(result.calibration.wait_samples, 1)
        stale = replace(
            history[0],
            response_ready_at=(self.now - timedelta(seconds=100)).isoformat(),
            accepted_at=(self.now - timedelta(seconds=125)).isoformat(),
        )
        self.assertEqual(
            self.signals(history=[stale]).calibration.unknown_wait_sandboxes, 1
        )

    def test_impossible_advisory_memory_is_unknown_without_losing_ownership(self):
        for memory in (2**63, 10**1000, -1, True, 1.5):
            with self.subTest(memory_type=type(memory).__name__):
                raw = self.entry.to_dict()
                raw['memory_observation'] = {'memory_bytes': memory, 'sampled_at': self.now.isoformat()}
                parsed = SandboxInventoryEntry.from_dict(raw)
                self.assertEqual(parsed.sandbox_id, 's')
                self.assertIsNone(parsed.memory_observation)
                signals = self.signals(heartbeats=[replace(self.heartbeat, inventory=(parsed,))])
                self.assertEqual(signals.calibration.unknown_memory_sandboxes, 1)
                self.assertEqual(signals.calibration.resources.memory_mb, self.shape.memory_mb)

    def test_byte_to_mib_rounding_remains_exact_at_integer_boundary(self):
        for memory, expected in ((2**53 + 1, 2**33 + 1), (2**63 - 1, 2**43)):
            sample = SandboxMemoryObservation(memory, self.now.isoformat())
            heartbeat = replace(self.heartbeat, inventory=(replace(self.entry, memory_observation=sample),))
            self.assertEqual(observed_memory_mb(self.route, [heartbeat], now=self.now, max_age_seconds=60), expected)

    def test_inventory_observation_is_optional_and_malformed_advice_does_not_erase_owner(
        self,
    ):
        legacy = replace(self.entry, memory_observation=None)
        self.assertNotIn("memory_observation", legacy.to_dict())
        self.assertEqual(
            SandboxInventoryEntry.from_dict(self.entry.to_dict()), self.entry
        )
        malformed = {**self.entry.to_dict(), "memory_observation": {"memory_bytes": -1}}
        parsed = SandboxInventoryEntry.from_dict(malformed)
        self.assertEqual(parsed.sandbox_id, "s")
        self.assertIsNone(parsed.memory_observation)
