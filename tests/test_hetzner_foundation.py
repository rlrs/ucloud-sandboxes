import unittest
from pathlib import Path

from scripts import setup_hetzner_foundation as foundation


class HetznerFoundationTests(unittest.TestCase):
    def test_gateway_firewall_exposes_only_web_by_default(self) -> None:
        rules = foundation._gateway_firewall_rules([])

        self.assertEqual([rule["port"] for rule in rules], ["80", "443"])
        self.assertTrue(
            all(rule["source_ips"] == ["0.0.0.0/0", "::/0"] for rule in rules)
        )

    def test_gateway_firewall_limits_ssh_to_normalized_operator_networks(
        self,
    ) -> None:
        sources = foundation._ip_networks(
            ["198.51.100.24/32", "2001:db8::17/64", "198.51.100.24/32"]
        )

        self.assertEqual(sources, ["198.51.100.24/32", "2001:db8::/64"])
        rules = foundation._gateway_firewall_rules(sources)
        self.assertEqual(rules[0]["port"], "22")
        self.assertEqual(rules[0]["source_ips"], sources)
        self.assertEqual([rule["port"] for rule in rules[1:]], ["80", "443"])

    def test_invalid_gateway_ssh_source_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "gateway SSH source CIDR"):
            foundation._ip_networks(["not-a-network"])

    def test_gateway_nat_allows_workers_through_docker_forward_chain(self) -> None:
        installer = (
            Path(__file__).parents[1] / "scripts" / "install_hetzner_gateway.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("iptables -I DOCKER-USER 1 -j UCLOUD-SANDBOXES", installer)
        self.assertIn("After=docker.service network-online.target", installer)
        self.assertIn(
            'oifname "$public_if" ip saddr 10.42.0.0/24 masquerade',
            installer,
        )

    def test_gateway_installer_requires_distinct_role_checked_node_bundles(
        self,
    ) -> None:
        installer = (
            Path(__file__).parents[1] / "scripts" / "install_hetzner_gateway.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("/tmp/ucloud-sandboxes-sandbox-node-package.tar.gz", installer)
        self.assertIn("/tmp/ucloud-sandboxes-builder-node-package.tar.gz", installer)
        self.assertIn("actual_role != expected_role", installer)
        self.assertNotIn('install -m 0644 "$node_bundle"', installer)


if __name__ == "__main__":
    unittest.main()
