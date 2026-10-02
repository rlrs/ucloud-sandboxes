from dataclasses import replace
from datetime import timedelta
import unittest

from ucloud_sandboxes.consolidation import observed_memory_mb
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxInventoryEntry,
    SandboxMemoryObservation,
    utc_now,
)
from ucloud_sandboxes.routing import SandboxRoute


class MemoryObservationTests(unittest.TestCase):
    """The consolidation memory observation is fenced to one owner incarnation."""

    def setUp(self):
        self.now = utc_now()
        self.shape = ResourceQuantity(vcpu=2, memory_mb=2048, disk_mb=8000)
        self.route = SandboxRoute(
            sandbox_id="s",
            node_id="n",
            job_id="j",
            node_url="http://n",
            state="parked",
            resources=self.shape,
            spec={"id": "s"},
            generation=1,
            create_operation_id="create-test-route",
            spec_hash="a" * 64,
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

    def observe(self, heartbeat):
        return observed_memory_mb(
            self.route, [heartbeat], now=self.now, max_age_seconds=60
        )

    def test_exact_owner_observation_is_reported_in_mib(self):
        self.assertEqual(self.observe(self.heartbeat), 512)

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
                self.assertIsNone(self.observe(heartbeat))

    def test_impossible_advisory_memory_is_unknown_without_losing_ownership(self):
        for memory in (2**63, 10**1000, -1, True, 1.5):
            with self.subTest(memory_type=type(memory).__name__):
                raw = self.entry.to_dict()
                raw["memory_observation"] = {
                    "memory_bytes": memory,
                    "sampled_at": self.now.isoformat(),
                }
                parsed = SandboxInventoryEntry.from_dict(raw)
                self.assertEqual(parsed.sandbox_id, "s")
                self.assertIsNone(parsed.memory_observation)
                self.assertIsNone(
                    self.observe(replace(self.heartbeat, inventory=(parsed,)))
                )

    def test_byte_to_mib_rounding_remains_exact_at_integer_boundary(self):
        for memory, expected in ((2**53 + 1, 2**33 + 1), (2**63 - 1, 2**43)):
            sample = SandboxMemoryObservation(memory, self.now.isoformat())
            heartbeat = replace(
                self.heartbeat,
                inventory=(replace(self.entry, memory_observation=sample),),
            )
            self.assertEqual(self.observe(heartbeat), expected)

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
