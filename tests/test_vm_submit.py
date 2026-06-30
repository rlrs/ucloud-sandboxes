import unittest

from ucloud_sandboxes.vm_submit import (
    VmApplicationRef,
    VmProductRef,
    VmSubmissionOptions,
    VmTimeAllocation,
    bulk_submission_payload,
)


class VmSubmitTests(unittest.TestCase):
    def test_builds_ucloud_vm_submission_payload(self) -> None:
        options = VmSubmissionOptions(
            name="ucloud-sandbox-node-1",
            hostname="sandbox-node-1",
            private_network_id="12345327",
            product=VmProductRef(
                id="cpu-amd-zen5-2-vcpu",
                category="cpu-amd-zen5",
                provider="ucloud",
            ),
            application=VmApplicationRef(name="vm-ubuntu", version="24.04"),
            disk_gb=50,
            time_allocation=VmTimeAllocation(hours=2, minutes=30),
            labels={"pool": "default"},
        )

        payload = options.bulk_payload()
        item = payload["items"][0]

        self.assertEqual(payload["type"], "bulk")
        self.assertEqual(item["name"], "ucloud-sandbox-node-1")
        self.assertEqual(item["hostname"], "sandbox-node-1")
        self.assertEqual(item["application"], {"name": "vm-ubuntu", "version": "24.04"})
        self.assertEqual(
            item["product"],
            {
                "id": "cpu-amd-zen5-2-vcpu",
                "category": "cpu-amd-zen5",
                "provider": "ucloud",
            },
        )
        self.assertFalse(item["sshEnabled"])
        self.assertEqual(item["parameters"]["diskSize"]["value"], 50)
        self.assertEqual(item["timeAllocation"], {"hours": 2, "minutes": 30, "seconds": 0})
        self.assertEqual(item["resources"], [{"type": "private_network", "id": "12345327"}])
        self.assertEqual(item["labels"], {"pool": "default"})

    def test_can_request_ucloud_ssh_when_app_supports_it(self) -> None:
        item = VmSubmissionOptions(
            name="node",
            hostname="sandbox-node-1",
            private_network_id=None,
            ssh_enabled=True,
        ).job_item()

        self.assertTrue(item["sshEnabled"])

    def test_can_build_payload_without_private_network_when_explicit(self) -> None:
        item = VmSubmissionOptions(
            name="node",
            hostname="sandbox-node-1",
            private_network_id=None,
        ).job_item()

        self.assertEqual(item["resources"], [])

    def test_can_attach_public_link_to_vm_port(self) -> None:
        item = VmSubmissionOptions(
            name="gateway",
            hostname="sandbox-gateway-1",
            private_network_id="net-1",
            public_link_id="link-1",
            public_link_port=8090,
        ).job_item()

        self.assertEqual(
            item["resources"],
            [
                {"type": "private_network", "id": "net-1"},
                {"type": "ingress", "id": "link-1", "port": 8090},
            ],
        )

    def test_bulk_submission_payload_supports_multiple_vm_items(self) -> None:
        payload = bulk_submission_payload(
            [
                VmSubmissionOptions(
                    name="node-1",
                    hostname="sandbox-node-1",
                    private_network_id=None,
                ),
                VmSubmissionOptions(
                    name="node-2",
                    hostname="sandbox-node-2",
                    private_network_id=None,
                ),
            ]
        )

        self.assertEqual(payload["type"], "bulk")
        self.assertEqual(len(payload["items"]), 2)
        self.assertEqual(payload["items"][1]["name"], "node-2")

    def test_rejects_bad_hostname(self) -> None:
        with self.assertRaises(ValueError):
            VmSubmissionOptions(
                name="node",
                hostname="Bad_Name",
                private_network_id="net-1",
            ).job_item()


if __name__ == "__main__":
    unittest.main()
