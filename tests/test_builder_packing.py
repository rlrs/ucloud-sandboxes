from dataclasses import replace
import unittest

from ucloud_sandboxes import control_plane
from ucloud_sandboxes.images import DEFAULT_MAX_ACTIVE_IMAGE_BUILDS
from ucloud_sandboxes.build_admission import BUILD_ADMISSION_CAPACITY_LABEL
from tests.test_control_plane import build_heartbeat


def builder(job_id: str, active: int):
    return replace(build_heartbeat(node_id=f"node-{job_id}", job_id=job_id, node_url=f"http://node-{job_id}:8090"), active_image_builds=active)


class BuilderPackingTests(unittest.TestCase):
    def pick(self, *builders):
        selected = control_plane._reserve_builder_candidate(list(builders), {}, reserve=False)
        return selected.job_id if selected else None

    def test_builds_pack_onto_the_oldest_builder_with_a_free_slot(self):
        slots = DEFAULT_MAX_ACTIVE_IMAGE_BUILDS
        # A trickle always lands on the oldest builder; newer ones stay idle.
        self.assertEqual(self.pick(builder("200", 0), builder("100", 0)), "100")
        # Fill a busy builder before waking an idle one.
        self.assertEqual(self.pick(builder("100", 0), builder("200", slots - 1)), "200")
        # A full builder yields to one with a free slot.
        self.assertEqual(self.pick(builder("100", slots), builder("200", 1)), "200")
        # Busy fleets leave new work pending instead of pinning it to a queue.
        self.assertIsNone(self.pick(builder("100", slots + 2), builder("200", slots + 1)))
        self.assertIsNone(self.pick(builder("100", slots), builder("200", slots)))

    def test_pipeline_uses_idle_peers_before_building_a_publication_queue(self):
        def pipeline(job, active):
            return replace(builder(job, active), labels={BUILD_ADMISSION_CAPACITY_LABEL: "6"})
        self.assertEqual(self.pick(pipeline("100", 0), pipeline("200", 1)), "200")
        self.assertEqual(self.pick(pipeline("100", 0), pipeline("200", 2)), "100")
        self.assertEqual(self.pick(pipeline("100", 5), pipeline("200", 3)), "200")
        self.assertIsNone(self.pick(pipeline("100", 6), pipeline("200", 6)))

    def test_pipeline_spreading_counts_dispatches_after_live_sample(self):
        from unittest.mock import patch
        nodes = [replace(builder(str(n), 0), labels={BUILD_ADMISSION_CAPACITY_LABEL: "6"})
                 for n in range(3)]
        with patch.dict(control_plane._BUILDER_DISPATCH_COUNTS, {}, clear=True), \
             patch.dict(control_plane._BUILDER_DISPATCH_INFLIGHT, {}, clear=True):
            chosen = [control_plane._reserve_builder_candidate(nodes, {}, reserve=True).job_id for _ in range(9)]
            self.assertEqual(chosen[:6], ["0", "0", "1", "1", "2", "2"])
            self.assertEqual(chosen[6:], ["0", "1", "2"])


if __name__ == "__main__":
    unittest.main()
