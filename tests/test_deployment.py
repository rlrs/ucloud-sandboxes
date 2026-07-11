import unittest

from ucloud_sandboxes.deployment import agent_version_is_compatible


class DeploymentTests(unittest.TestCase):
    def test_agent_patch_compatibility_supports_rolling_gateway_updates(self) -> None:
        self.assertTrue(agent_version_is_compatible("0.3.42", expected="0.3.44"))
        self.assertTrue(agent_version_is_compatible("0.3.43", expected="0.3.44"))
        self.assertTrue(agent_version_is_compatible("0.3.44", expected="0.3.44"))

    def test_agent_compatibility_rejects_unsafe_versions(self) -> None:
        self.assertFalse(agent_version_is_compatible("0.3.41", expected="0.3.44"))
        self.assertFalse(agent_version_is_compatible("0.3.45", expected="0.3.44"))
        self.assertFalse(agent_version_is_compatible("0.4.0", expected="0.3.44"))
        self.assertFalse(agent_version_is_compatible("0.3.44-dev", expected="0.3.44"))
        self.assertFalse(agent_version_is_compatible("", expected="0.3.44"))


if __name__ == "__main__":
    unittest.main()
