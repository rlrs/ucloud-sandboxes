import unittest

from ucloud_sandboxes.models import vm_job_from_payload


class VmJobParsingTests(unittest.TestCase):
    def test_parses_ucloud_vm_job_shape(self) -> None:
        payload = {
            "id": "12345311",
            "createdAt": 1782638330055,
            "owner": {"project": "project-1"},
            "updates": [{"status": "queue full"}],
            "specification": {
                "product": {
                    "id": "cpu-amd-zen5-2-vcpu",
                    "category": "cpu-amd-zen5",
                    "provider": "ucloud",
                },
                "application": {"name": "vm-ubuntu", "version": "24.04"},
                "hostname": "ubuntu-8263",
                "labels": {"ucloud-sandboxes/deployment": "prod-a"},
                "resources": [{"type": "private_network", "id": "net-1"}],
                "parameters": {
                    "diskSize": {"value": 50},
                },
            },
            "status": {
                "state": "IN_QUEUE",
                "jobParametersJson": {
                    "request": {
                        "sshEnabled": False,
                        "resolvedProduct": {
                            "cpu": 2,
                            "memoryInGigs": 6,
                        },
                        "resolvedSupport": {
                            "support": {
                                "queueStatus": "FULL",
                            },
                        },
                    },
                    "machineType": {"cpu": 2, "memoryInGigs": 6},
                },
            },
        }

        job = vm_job_from_payload(payload)

        self.assertEqual(job.id, "12345311")
        self.assertEqual(job.project_id, "project-1")
        self.assertTrue(job.is_vm)
        self.assertEqual(job.state, "IN_QUEUE")
        self.assertEqual(job.product_id, "cpu-amd-zen5-2-vcpu")
        self.assertEqual(job.cpu, 2)
        self.assertEqual(job.memory_gb, 6)
        self.assertEqual(job.disk_gb, 50)
        self.assertEqual(job.labels, {"ucloud-sandboxes/deployment": "prod-a"})
        self.assertEqual(job.private_network_ids, ("net-1",))
        self.assertEqual(job.queue_status, "FULL")
        self.assertFalse(job.ssh_enabled)

    def test_estimates_cpu_from_vm_product_id_when_resolved_product_is_absent(self) -> None:
        payload = {
            "id": "123",
            "specification": {
                "product": {
                    "id": "cpu-amd-zen5-2-vcpu",
                    "category": "cpu-amd-zen5",
                },
                "application": {"name": "vm-ubuntu", "version": "24.04"},
            },
            "status": {"state": "IN_QUEUE"},
        }

        job = vm_job_from_payload(payload)

        self.assertEqual(job.cpu, 2)


if __name__ == "__main__":
    unittest.main()
