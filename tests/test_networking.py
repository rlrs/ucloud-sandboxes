import unittest

from ucloud_sandboxes.providers.ucloud.payloads import (
    PrivateNetworkAttachment,
    PublicLinkAttachment,
    apply_private_network_attachment,
    apply_public_link_attachment,
)


class NetworkingTests(unittest.TestCase):
    def test_private_network_attachment_sets_hostname_and_resource(self) -> None:
        item = {
            "name": "node-one",
            "resources": [{"type": "ingress", "id": "link-1", "port": 8080}],
        }
        attachment = PrivateNetworkAttachment(
            network_id="net-1",
            hostname="sandbox-node-1",
        )

        updated = apply_private_network_attachment(item, attachment)

        self.assertEqual(updated["hostname"], "sandbox-node-1")
        self.assertEqual(
            updated["resources"],
            [
                {"type": "ingress", "id": "link-1", "port": 8080},
                {"type": "private_network", "id": "net-1"},
            ],
        )
        self.assertNotIn("hostname", item)

    def test_private_network_attachment_is_idempotent(self) -> None:
        item = {
            "resources": [
                {"type": "private_network", "id": "net-1"},
            ]
        }

        updated = apply_private_network_attachment(
            item,
            PrivateNetworkAttachment(network_id="net-1", hostname="sandbox-node-1"),
        )

        self.assertEqual(
            updated["resources"], [{"type": "private_network", "id": "net-1"}]
        )

    def test_public_link_attachment_sets_ingress_resource_with_port(self) -> None:
        item = {"resources": [{"type": "private_network", "id": "net-1"}]}

        updated = apply_public_link_attachment(
            item,
            PublicLinkAttachment(link_id="link-1", port=8090),
        )

        self.assertEqual(
            updated["resources"],
            [
                {"type": "private_network", "id": "net-1"},
                {"type": "ingress", "id": "link-1", "port": 8090},
            ],
        )
        self.assertEqual(
            item["resources"], [{"type": "private_network", "id": "net-1"}]
        )

    def test_public_link_attachment_updates_existing_resource_port(self) -> None:
        item = {"resources": [{"type": "ingress", "id": "link-1"}]}

        updated = apply_public_link_attachment(
            item,
            PublicLinkAttachment(link_id="link-1", port=8080),
        )

        self.assertEqual(
            updated["resources"], [{"type": "ingress", "id": "link-1", "port": 8080}]
        )

    def test_rejects_invalid_hostname(self) -> None:
        with self.assertRaises(ValueError):
            PrivateNetworkAttachment(network_id="net-1", hostname="Bad_Name")

    def test_rejects_invalid_public_link_port(self) -> None:
        with self.assertRaises(ValueError):
            PublicLinkAttachment(link_id="link-1", port=0)


if __name__ == "__main__":
    unittest.main()
