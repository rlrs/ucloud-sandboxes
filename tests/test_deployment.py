import unittest

from ucloud_sandboxes.deployment import agent_version_is_schedulable


class DeploymentTests(unittest.TestCase):
    def test_scheduling_requires_exact_release(self) -> None:
        self.assertFalse(
            agent_version_is_schedulable("0.3.76", expected="0.3.77")
        )
        self.assertTrue(
            agent_version_is_schedulable("0.3.77", expected="0.3.77")
        )
        self.assertFalse(
            agent_version_is_schedulable("0.3.78", expected="0.3.77")
        )


if __name__ == "__main__":
    unittest.main()
