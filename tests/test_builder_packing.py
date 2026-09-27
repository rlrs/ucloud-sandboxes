from dataclasses import replace
import unittest

from ucloud_sandboxes import control_plane
from ucloud_sandboxes.images import DEFAULT_MAX_ACTIVE_IMAGE_BUILDS
from tests.test_control_plane import build_heartbeat


def builder(job_id: str, active: int):
    return replace(build_heartbeat(node_id=f"node-{job_id}", job_id=job_id, node_url=f"http://node-{job_id}:8090"), active_image_builds=active)


class BuilderPackingTests(unittest.TestCase):
    def pick(self, *builders):
        return control_plane._reserve_builder_candidate(list(builders), {}, reserve=False).job_id

    def test_builds_pack_onto_the_oldest_builder_with_a_free_slot(self):
        slots = DEFAULT_MAX_ACTIVE_IMAGE_BUILDS
        # A trickle always lands on the oldest builder; newer ones stay idle.
        self.assertEqual(self.pick(builder("200", 0), builder("100", 0)), "100")
        # Fill a busy builder before waking an idle one.
        self.assertEqual(self.pick(builder("100", 0), builder("200", slots - 1)), "200")
        # A full builder yields to one with a free slot.
        self.assertEqual(self.pick(builder("100", slots), builder("200", 1)), "200")
        # Only when every builder is full does the shortest queue win.
        self.assertEqual(self.pick(builder("100", slots + 2), builder("200", slots + 1)), "200")


if __name__ == "__main__":
    unittest.main()
