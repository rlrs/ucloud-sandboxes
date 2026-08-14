import unittest
from pathlib import Path
import subprocess

from scripts import setup_hetzner_foundation as foundation


class HetznerFoundationTests(unittest.TestCase):
    def test_gateway_firewall_defaults_and_operator_access(self) -> None:
        defaults = foundation._gateway_firewall_rules([])
        self.assertEqual([rule["port"] for rule in defaults], ["80", "443"])

        sources = foundation._ip_networks(
            ["198.51.100.24/32", "2001:db8::17/64", "198.51.100.24/32"]
        )
        self.assertEqual(sources, ["198.51.100.24/32", "2001:db8::/64"])
        rules = foundation._gateway_firewall_rules(sources)
        self.assertEqual((rules[0]["port"], rules[0]["source_ips"]), ("22", sources))
        with self.assertRaisesRegex(ValueError, "gateway SSH source CIDR"):
            foundation._ip_networks(["not-a-network"])

    def test_gateway_installer_has_valid_bash_syntax(self) -> None:
        installer = Path(__file__).parents[1] / "scripts" / "install_hetzner_gateway.sh"
        subprocess.run(["bash", "-n", str(installer)], check=True)


if __name__ == "__main__":
    unittest.main()
