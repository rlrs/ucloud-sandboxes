from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.deployment import AGENT_VERSION_LABEL, package_version
from ucloud_sandboxes.models import NodeRuntimeMetrics, ScalePolicy, VmJob, utc_now
from ucloud_sandboxes.registry import HeartbeatStore, merge_jobs_and_heartbeats


class RegistryTests(unittest.TestCase):
    def test_marks_provisioning_node_without_heartbeat(self) -> None:
        job = VmJob(
            id="123",
            project_id="project-1",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="IN_QUEUE",
            cpu=2,
            labels={AGENT_VERSION_LABEL: package_version()},
        )

        nodes = merge_jobs_and_heartbeats(
            [job],
            {},
            ScalePolicy(),
        )

        self.assertTrue(nodes[0].is_provisioning)

    def test_marks_suspended_vm_as_provisioning(self) -> None:
        job = VmJob(
            id="123",
            project_id="project-1",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-16-vcpu",
            product_category="cpu-amd-zen5",
            state="SUSPENDED",
            cpu=16,
            disk_gb=250,
            labels={AGENT_VERSION_LABEL: package_version()},
        )

        nodes = merge_jobs_and_heartbeats(
            [job],
            {},
            ScalePolicy(),
        )

        self.assertTrue(nodes[0].is_provisioning)

    def test_unversioned_provisioning_node_is_incompatible(self) -> None:
        job = VmJob(
            id="123",
            project_id="project-1",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="IN_QUEUE",
            cpu=2,
        )

        nodes = merge_jobs_and_heartbeats(
            [job],
            {},
            ScalePolicy(),
        )

        self.assertFalse(nodes[0].agent_version_compatible)
        self.assertFalse(nodes[0].is_provisioning)

    def test_heartbeat_store_roundtrip(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            heartbeat = build_heartbeat(
                job_id="job-1",
                node_id="node-1",
                node_url="http://node-1:8090",
                active_sandboxes=2,
                active_image_builds=1,
                agent_version="0.1.0-test",
                deployment_id="prod-a",
                init_version="init-1",
                capabilities=("sandbox", "image-build"),
                labels={"role": "worker"},
                runtime_metrics=NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=10.0,
                    cpu_vcpu=0.2,
                    cpu_count=2,
                    memory_total_mb=6144,
                    memory_used_mb=1024,
                    memory_available_mb=5120,
                    memory_percent=16.6666666667,
                ),
            )

            store = HeartbeatStore(path)
            store.upsert(heartbeat)
            loaded = store.load()

            self.assertIn("job-1", loaded)
            self.assertEqual(loaded["job-1"].node_id, "node-1")
            self.assertEqual(loaded["job-1"].node_url, "http://node-1:8090")
            self.assertEqual(loaded["job-1"].agent_version, "0.1.0-test")
            self.assertEqual(loaded["job-1"].deployment_id, "prod-a")
            self.assertEqual(loaded["job-1"].init_version, "init-1")
            self.assertEqual(loaded["job-1"].active_sandboxes, 2)
            self.assertEqual(loaded["job-1"].active_image_builds, 1)
            self.assertEqual(loaded["job-1"].capabilities, ("sandbox", "image-build"))
            self.assertEqual(loaded["job-1"].labels, {"role": "worker"})
            self.assertIsNotNone(loaded["job-1"].runtime_metrics)
            assert loaded["job-1"].runtime_metrics is not None
            self.assertEqual(loaded["job-1"].runtime_metrics.cpu_percent, 10.0)
            self.assertEqual(loaded["job-1"].runtime_metrics.memory_used_mb, 1024)

    def test_heartbeat_store_removes_jobs(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            store = HeartbeatStore(path)
            store.save(
                {
                    "job-1": build_heartbeat(job_id="job-1", node_id="node-1"),
                    "job-2": build_heartbeat(job_id="job-2", node_id="node-2"),
                }
            )

            removed = store.remove(("job-1", "missing"))
            loaded = store.load()

            self.assertEqual(tuple(removed), ("job-1",))
            self.assertNotIn("job-1", loaded)
            self.assertIn("job-2", loaded)

    def test_heartbeat_store_tracks_idle_since_transition(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            store = HeartbeatStore(path)
            busy_at = utc_now()
            idle_at = busy_at + timedelta(seconds=30)
            later_at = idle_at + timedelta(seconds=30)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=1,
                    now=busy_at,
                )
            )
            self.assertIsNone(store.load()["job-1"].idle_since)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=0,
                    now=idle_at,
                )
            )
            self.assertEqual(store.load()["job-1"].idle_since, idle_at)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=0,
                    now=later_at,
                )
            )
            self.assertEqual(store.load()["job-1"].idle_since, idle_at)

    def test_heartbeat_store_treats_image_build_as_active_work(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            store = HeartbeatStore(path)
            busy_at = utc_now()
            idle_at = busy_at + timedelta(seconds=30)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_image_builds=1,
                    now=busy_at,
                )
            )
            self.assertIsNone(store.load()["job-1"].idle_since)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_image_builds=0,
                    now=idle_at,
                )
            )
            self.assertEqual(store.load()["job-1"].idle_since, idle_at)


if __name__ == "__main__":
    unittest.main()
