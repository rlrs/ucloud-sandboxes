import unittest

from ucloud_sandboxes.build_admission import (
    BUILD_ADMISSION_CAPACITY_LABEL,
    build_admission_capacity,
)
from ucloud_sandboxes.images import DEFAULT_MAX_ACTIVE_IMAGE_BUILDS
from ucloud_sandboxes.models import NodeHeartbeat, utc_now
from ucloud_sandboxes.registry import heartbeat_from_dict, heartbeat_to_dict


class BuildAdmissionContractTests(unittest.TestCase):
    def test_legacy_nodes_keep_four_slots(self):
        self.assertEqual(build_admission_capacity({}), DEFAULT_MAX_ACTIVE_IMAGE_BUILDS)
        self.assertEqual(build_admission_capacity({"unrelated": "6"}), 4)

    def test_capacity_hint_round_trips_existing_heartbeat_schema(self):
        for capacity in (0, 1, 2, 4, 6, 32):
            with self.subTest(capacity=capacity):
                heartbeat = NodeHeartbeat(
                    node_id="builder", job_id="job", updated_at=utc_now(),
                    active_sandboxes=0, deployment_id="deployment",
                    labels={BUILD_ADMISSION_CAPACITY_LABEL: str(capacity)},
                )
                loaded = heartbeat_from_dict(heartbeat_to_dict(heartbeat))
                self.assertIsNotNone(loaded)
                self.assertEqual(build_admission_capacity(loaded.labels), capacity)

    def test_present_malformed_hint_closes_new_admission(self):
        for malformed in (
            "", "-1", "+6", "6.0", " 6", "6 ", "06", "00", "٦",
            "2147483648", "9" * 5000, None, 6, True,
        ):
            with self.subTest(value=repr(malformed)[:40]):
                self.assertEqual(
                    build_admission_capacity({BUILD_ADMISSION_CAPACITY_LABEL: malformed}),
                    0,
                )


if __name__ == "__main__":
    unittest.main()
