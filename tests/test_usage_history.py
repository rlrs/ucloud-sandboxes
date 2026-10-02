from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from ucloud_sandboxes.models import (
    ResourceQuantity,
    ScalePolicy,
    SandboxDemand,
    SandboxMemoryObservation,
    SandboxPlacementRequest,
)
from ucloud_sandboxes.policy import evaluate_scale
from ucloud_sandboxes.usage_history import (
    RETENTION_SECONDS,
    UsageHistory,
    demand_with_usage_forecast,
    record_inventory_usage,
)

IMAGE = "10.42.0.2:5000/ucloud-managed/pool:latest@sha256:" + "a" * 64
SAME_IMAGE_BY_DIGEST = "10.42.0.2:5000/ucloud-managed/pool@sha256:" + "a" * 64
OTHER_IMAGE = "python@sha256:" + "b" * 64
SHAPE = ResourceQuantity(4, 8192, 10240)
NOW = 1_000_000.0


class UsageHistoryTests(unittest.TestCase):
    def test_forecast_is_the_observed_peak_plus_margin_capped_at_the_request(self):
        history = UsageHistory()
        history.observe(IMAGE, SHAPE, 1200, NOW)
        history.observe(IMAGE, SHAPE, 1600, NOW + 5)
        history.observe(IMAGE, SHAPE, 900, NOW + 10)  # the peak is kept
        self.assertEqual(history.forecast_memory_mb(SAME_IMAGE_BY_DIGEST, SHAPE, NOW), 2000)
        small = ResourceQuantity(4, 1024, 10240)
        history.observe(IMAGE, small, 1000, NOW)
        self.assertEqual(history.forecast_memory_mb(IMAGE, small, NOW), 1024)

    def test_lookup_falls_back_to_same_image_then_same_shape(self):
        history = UsageHistory()
        history.observe(IMAGE, ResourceQuantity(1, 2048, 4096), 800, NOW)
        history.observe(OTHER_IMAGE, SHAPE, 3000, NOW)
        # Same image at another shape wins over the same shape of another image.
        self.assertEqual(history.forecast_memory_mb(IMAGE, SHAPE, NOW), 1000)
        # An unseen image of a known shape uses that shape's peak.
        unseen = "unseen@sha256:" + "c" * 64
        self.assertEqual(history.forecast_memory_mb(unseen, SHAPE, NOW), 3750)
        self.assertIsNone(history.forecast_memory_mb(unseen, ResourceQuantity(2, 4096, 1), NOW))

    def test_entries_expire_after_the_retention_period(self):
        history = UsageHistory()
        history.observe(IMAGE, SHAPE, 1600, NOW)
        later = NOW + RETENTION_SECONDS + 1
        self.assertIsNone(history.forecast_memory_mb(IMAGE, SHAPE, later))
        history.prune(later)
        self.assertEqual(history.entries, {})

    def test_history_survives_a_restart_and_ignores_a_corrupt_file(self):
        with TemporaryDirectory() as raw:
            path = Path(raw) / "usage-history.json"
            history = UsageHistory(path)
            history.observe(IMAGE, SHAPE, 1600, NOW)
            history.save_if_due(NOW)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            restored = UsageHistory.load(path)
            self.assertEqual(restored.forecast_memory_mb(IMAGE, SHAPE, NOW), 2000)
            path.write_text("{not json")
            self.assertEqual(UsageHistory.load(path).entries, {})

    def test_records_only_running_sandboxes_with_a_known_image(self):
        history = UsageHistory()

        def entry(sandbox_id, state, mib):
            return SimpleNamespace(
                sandbox_id=sandbox_id, generation=1, state=state, resources=SHAPE,
                memory_observation=SandboxMemoryObservation(mib * 1024**2, "2026-09-26T00:00:00+00:00"),
            )

        heartbeat = SimpleNamespace(inventory=(
            entry("a", "running", 1500), entry("b", "parked", 9000), entry("c", "running", 9000),
        ))
        record_inventory_usage(history, [heartbeat], {("a", 1): IMAGE, ("b", 1): IMAGE}, NOW)
        self.assertEqual(history.forecast_memory_mb(IMAGE, SHAPE, NOW), 1875)

    def test_cold_demand_uses_history_and_the_initial_disk_claim(self):
        history = UsageHistory()
        history.observe(IMAGE, SHAPE, 1600, NOW)
        demand = SandboxDemand(
            placement_requests=(SandboxPlacementRequest(SHAPE, image=OTHER_IMAGE + "x"),),
            prepared_placement_requests=(
                SandboxPlacementRequest(SHAPE, count=73, image=IMAGE),
                SandboxPlacementRequest(ResourceQuantity(1, 2048, 4096), count=2),
            ),
        )
        forecast = demand_with_usage_forecast(demand, history, initial_disk_claim_mb=576, now=NOW)
        prepared = forecast.prepared_placement_requests
        self.assertEqual(prepared[0].resources, ResourceQuantity(4, 2000, 576))
        self.assertEqual(prepared[0].count, 73)
        # Nothing observed for this shape: full memory, disk still at the claim.
        self.assertEqual(prepared[1].resources, ResourceQuantity(1, 2048, 576))
        # A pending create of an unseen image of a known shape uses the shape's peak.
        self.assertEqual(forecast.placement_requests[0].resources.memory_mb, 2000)
        unchanged = demand_with_usage_forecast(demand, UsageHistory(), initial_disk_claim_mb=0, now=NOW)
        self.assertEqual(unchanged.prepared_placement_requests, demand.prepared_placement_requests)

    def test_cold_start_scales_to_remembered_usage_not_requests(self):
        # The 2026-09-26 agentic test on Hetzner: CCX63 workers (capped at 3),
        # about 100 announced sandboxes of 4 vCPU / 4 GiB / 11,328 MB that each
        # used about 1.5 GB. Requests bought three workers.
        node = ResourceQuantity(48, 184320, 617472)
        policy = ScalePolicy(default_node_resources=node, max_nodes=3,
                             max_create_per_cycle=3, max_provisioning_nodes=3)
        shape = ResourceQuantity(4, 4096, 11328)
        demand = SandboxDemand(prepared_placement_requests=(
            SandboxPlacementRequest(shape, count=100, image=IMAGE),
        ))
        self.assertEqual(evaluate_scale([], demand, policy).creates, 3)
        history = UsageHistory()
        history.observe(IMAGE, shape, 1500, NOW)
        forecast = demand_with_usage_forecast(demand, history, initial_disk_claim_mb=576, now=NOW)
        # 100 x 1,875 MB / 0.8 target utilization needs two workers' memory.
        self.assertEqual(evaluate_scale([], forecast, policy).creates, 2)


if __name__ == "__main__":
    unittest.main()
