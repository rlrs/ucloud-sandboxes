import argparse
from contextlib import redirect_stdout
from dataclasses import dataclass
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes import cli
from ucloud_sandboxes.cli import (
    find_ucloud_ssh_key,
    metrics_path_from_args,
    read_init_authorized_keys,
    read_public_ssh_key_file,
    should_include_job,
    submitted_job_ids,
    vm_submission_options_from_args,
)
from ucloud_sandboxes.config import AutoscalerConfig
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, ScalePolicy, VmJob, utc_now
from ucloud_sandboxes.registry import HeartbeatStore
from ucloud_sandboxes.routing import RoutingStore


@dataclass(frozen=True)
class FakeProbeReport:
    ok: bool = False
    runtime_name: str = "runsc"
    image: str = "busybox"
    executed: bool = True
    results: tuple = ()

    def to_dict(self) -> dict:
        return {"ok": self.ok}


class FailingProbe:
    def __init__(self, **_kwargs) -> None:
        pass

    def run(self) -> FakeProbeReport:
        return FakeProbeReport(ok=False)


class CliTests(unittest.TestCase):
    def test_private_network_config_filters_auto_discovered_pool_nodes(self) -> None:
        config = AutoscalerConfig.default(project_id="project-1")
        config = AutoscalerConfig(
            project_id=config.project_id,
            job_name_prefix=config.job_name_prefix,
            private_network_id="net-1",
            node_hostname_prefix=config.node_hostname_prefix,
            ucloud_session_file=config.ucloud_session_file,
            state_dir=config.state_dir,
            policy=config.policy,
        )
        matching = VmJob(
            id="job-1",
            project_id="project-1",
            name="ucloud-sandbox-node-1",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="RUNNING",
            private_network_ids=("net-1",),
        )
        wrong_network = VmJob(
            id="job-2",
            project_id="project-1",
            name="ucloud-sandbox-node-2",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="RUNNING",
            private_network_ids=("net-2",),
        )

        self.assertTrue(should_include_job(matching, config, set(), False))
        self.assertFalse(should_include_job(wrong_network, config, set(), False))
        self.assertTrue(should_include_job(wrong_network, config, {"job-2"}, False))

    def test_metrics_path_defaults_to_route_file_directory(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            state_dir="/tmp/default-state",
            ucloud_session_file="/tmp/session.json",
        )

        self.assertEqual(
            metrics_path_from_args(
                argparse.Namespace(metrics_file=None),
                config,
                sibling_file=Path("/work/ucloud-sandboxes/state/routes.json"),
            ),
            Path("/work/ucloud-sandboxes/state/metrics.jsonl"),
        )

    def test_vm_submission_options_use_private_network_config(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            private_network_id="12345327",
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        args = argparse.Namespace(
            no_private_network=False,
            private_network_id=None,
            hostname_seed="123",
            hostname_prefix=None,
            hostname=None,
            name=None,
            label=[],
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            product_provider="ucloud",
            app_name="vm-ubuntu",
            app_version="24.04",
            disk_gb=50,
            time_hours=1,
            time_minutes=0,
            time_seconds=0,
            ssh=False,
            no_ssh=False,
            allow_duplicate_job=False,
        )

        options, seed = vm_submission_options_from_args(args, config)

        self.assertEqual(seed, "123")
        self.assertEqual(options.private_network_id, "12345327")
        self.assertEqual(options.hostname, "sandbox-node-123")
        self.assertEqual(options.name, "ucloud-sandbox-node-123")
        self.assertFalse(options.ssh_enabled)

    def test_vm_submission_options_can_request_ssh_explicitly(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            private_network_id="12345327",
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        args = argparse.Namespace(
            no_private_network=False,
            private_network_id=None,
            hostname_seed="123",
            hostname_prefix=None,
            hostname=None,
            name=None,
            label=[],
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            product_provider="ucloud",
            app_name="vm-ubuntu",
            app_version="24.04",
            disk_gb=50,
            time_hours=1,
            time_minutes=0,
            time_seconds=0,
            ssh=True,
            no_ssh=False,
            allow_duplicate_job=False,
        )

        options, _seed = vm_submission_options_from_args(args, config)

        self.assertTrue(options.ssh_enabled)

    def test_vm_submission_options_use_gateway_public_link_config(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        args = argparse.Namespace(
            no_private_network=False,
            private_network_id=None,
            no_public_link=False,
            public_link_id=None,
            public_link_port=None,
            mount=[],
            mount_ro=[],
            role="gateway",
            hostname_seed="gateway",
            hostname_prefix=None,
            hostname=None,
            name=None,
            label=[],
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            product_provider="ucloud",
            app_name="vm-ubuntu",
            app_version="24.04",
            disk_gb=50,
            time_hours=1,
            time_minutes=0,
            time_seconds=0,
            ssh=False,
            no_ssh=False,
            allow_duplicate_job=False,
        )

        options, _seed = vm_submission_options_from_args(args, config)

        self.assertEqual(options.public_link_id, "12345368")
        self.assertEqual(options.public_link_port, 8090)
        self.assertEqual(options.hostname, "sandbox-gateway-gateway")
        self.assertEqual(options.name, "ucloud-sandbox-gateway-gateway")
        self.assertNotIn("ucloud-sandboxes/node", options.job_item()["labels"])
        self.assertEqual(options.job_item()["labels"]["ucloud-sandboxes/gateway"], "true")
        self.assertIn(
            {"type": "ingress", "id": "12345368", "port": 8090},
            options.job_item()["resources"],
        )

    def test_vm_submission_options_include_project_file_mounts(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        args = argparse.Namespace(
            no_private_network=False,
            private_network_id=None,
            no_public_link=False,
            public_link_id=None,
            public_link_port=None,
            mount=["/1234567/ucloud-sandbox-registry"],
            mount_ro=["/1234567/shared-base-images"],
            role="gateway",
            hostname_seed="gateway",
            hostname_prefix=None,
            hostname=None,
            name=None,
            label=[],
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            product_provider="ucloud",
            app_name="vm-ubuntu",
            app_version="24.04",
            disk_gb=50,
            time_hours=1,
            time_minutes=0,
            time_seconds=0,
            ssh=False,
            no_ssh=False,
            allow_duplicate_job=False,
        )

        options, _seed = vm_submission_options_from_args(args, config)
        resources = options.job_item()["resources"]

        self.assertEqual(options.file_mounts[0].path, "/1234567/ucloud-sandbox-registry")
        self.assertFalse(options.file_mounts[0].read_only)
        self.assertEqual(resources[-2]["type"], "file")
        self.assertEqual(resources[-2]["path"], "/1234567/ucloud-sandbox-registry")
        self.assertFalse(resources[-2]["readOnly"])
        self.assertEqual(resources[-1]["path"], "/1234567/shared-base-images")
        self.assertTrue(resources[-1]["readOnly"])

    def test_node_role_does_not_consume_gateway_public_link_config(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        args = argparse.Namespace(
            no_private_network=False,
            private_network_id=None,
            no_public_link=False,
            public_link_id=None,
            public_link_port=None,
            role="node",
            hostname_seed="node",
            hostname_prefix=None,
            hostname=None,
            name=None,
            label=[],
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            product_provider="ucloud",
            app_name="vm-ubuntu",
            app_version="24.04",
            disk_gb=50,
            time_hours=1,
            time_minutes=0,
            time_seconds=0,
            ssh=False,
            no_ssh=False,
            allow_duplicate_job=False,
        )

        options, _seed = vm_submission_options_from_args(args, config)

        self.assertIsNone(options.public_link_id)
        self.assertEqual(
            options.job_item()["resources"],
            [{"type": "private_network", "id": "12345327"}],
        )

    def test_builder_role_uses_builder_identity_without_node_label(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            state_dir="/tmp/state",
        )
        args = argparse.Namespace(
            no_private_network=False,
            private_network_id=None,
            no_public_link=False,
            public_link_id=None,
            public_link_port=None,
            role="builder",
            hostname_seed="build",
            hostname_prefix=None,
            hostname=None,
            name=None,
            label=[],
            product_id="cpu-amd-zen5-16-vcpu",
            product_category="cpu-amd-zen5",
            product_provider="ucloud",
            app_name="vm-ubuntu",
            app_version="24.04",
            disk_gb=250,
            time_hours=1,
            time_minutes=0,
            time_seconds=0,
            ssh=False,
            no_ssh=False,
            allow_duplicate_job=False,
        )

        options, _seed = vm_submission_options_from_args(args, config)
        labels = options.job_item()["labels"]

        self.assertIsNone(options.public_link_id)
        self.assertEqual(options.hostname, "sandbox-builder-build")
        self.assertEqual(options.name, "ucloud-sandbox-builder-build")
        self.assertEqual(labels["ucloud-sandboxes/builder"], "true")
        self.assertNotIn("ucloud-sandboxes/node", labels)
        self.assertNotIn("ucloud-sandboxes/gateway", labels)

    def test_submitted_job_ids_extracts_bulk_response_ids(self) -> None:
        self.assertEqual(
            submitted_job_ids({"responses": [{"id": "1"}, {"id": "2"}]}),
            ["1", "2"],
        )

    def test_vm_init_authorized_keys_load_from_args_and_files(self) -> None:
        with TemporaryDirectory() as raw_dir:
            key_file = Path(raw_dir) / "gateway-init.pub"
            key_file.write_text(
                "# comment\nssh-ed25519 BBBB gateway-file\n\n",
                encoding="utf-8",
            )
            args = argparse.Namespace(
                init_authorized_key=["ssh-ed25519 AAAA gateway-arg"],
                init_authorized_key_file=[key_file],
            )

            self.assertEqual(
                read_init_authorized_keys(args),
                (
                    "ssh-ed25519 AAAA gateway-arg",
                    "ssh-ed25519 BBBB gateway-file",
                ),
            )

    def test_read_public_ssh_key_file_validates_single_openssh_key(self) -> None:
        with TemporaryDirectory() as raw_dir:
            key_file = Path(raw_dir) / "gateway-init.pub"
            key_file.write_text("ssh-ed25519 AAAA gateway\n", encoding="utf-8")

            self.assertEqual(read_public_ssh_key_file(key_file), "ssh-ed25519 AAAA gateway")

            key_file.write_text("ssh-ed25519 AAAA gateway\nssh-ed25519 BBBB other\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                read_public_ssh_key_file(key_file)

    def test_find_ucloud_ssh_key_matches_key_material(self) -> None:
        items = [
            {"id": "1", "specification": {"title": "other", "key": "ssh-ed25519 AAAA other"}},
            {"id": "2", "specification": {"title": "gateway", "key": "ssh-ed25519 BBBB gateway"}},
        ]

        self.assertEqual(
            find_ucloud_ssh_key(items, "ssh-ed25519 BBBB gateway"),
            items[1],
        )
        self.assertIsNone(find_ucloud_ssh_key(items, "ssh-ed25519 CCCC missing"))

    def test_autoscaler_loop_once_uses_route_file_pending_demand(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            route_file = root / "routes.json"
            RoutingStore(route_file).upsert_pending(
                "pending-one",
                ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
            )

            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler-loop",
                        "--project",
                        "project-1",
                        "--state-dir",
                        raw_dir,
                        "--route-file",
                        str(route_file),
                        "--jobs-file",
                        str(jobs_file),
                        "--no-private-network",
                        "--once",
                        "--output",
                        "json",
                    ]
                )

            payload = json.loads(output.getvalue())

        self.assertEqual(result, 0)
        self.assertEqual(payload["cycle"], 1)
        self.assertEqual(payload["decision"]["pendingResources"]["vcpu"], 1.0)
        self.assertEqual(payload["decision"]["actions"][0]["kind"], "create")
        self.assertEqual(payload["consumedPendingDemand"], [])
        self.assertEqual(payload["consumedPreparedCapacity"], [])

    def test_executing_autoscaler_loop_consumes_pending_demand_signal(self) -> None:
        submitted: list[tuple[str, dict]] = []

        class FakeUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, project_id: str, payload: dict) -> dict:
                submitted.append((project_id, payload))
                return {"responses": [{"id": "created-node"}]}

        original_client = cli.UCloudClient
        cli.UCloudClient = FakeUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                route_file = root / "routes.json"
                RoutingStore(route_file).upsert_pending(
                    "pending-one",
                    ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
                )

                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler-loop",
                            "--project",
                            "project-1",
                            "--state-dir",
                            raw_dir,
                            "--route-file",
                            str(route_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--no-private-network",
                            "--once",
                            "--execute",
                            "--output",
                            "json",
                        ]
                    )

                payload = json.loads(output.getvalue())
                remaining_demand = RoutingStore(route_file).pending_demand()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(payload["createdJobIds"], ["created-node"])
        self.assertEqual(
            [item["sandbox_id"] for item in payload["consumedPendingDemand"]],
            ["pending-one"],
        )
        self.assertEqual(payload["consumedPreparedCapacity"], [])
        self.assertEqual(remaining_demand.pending_resources, ResourceQuantity())

    def test_executing_autoscaler_loop_consumes_prepared_capacity_signal(self) -> None:
        submitted: list[tuple[str, dict]] = []

        class FakeUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, project_id: str, payload: dict) -> dict:
                submitted.append((project_id, payload))
                return {"responses": [{"id": "created-node"}]}

        original_client = cli.UCloudClient
        cli.UCloudClient = FakeUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                route_file = root / "routes.json"
                RoutingStore(route_file).upsert_prepared_capacity(
                    "eval-soon",
                    ResourceQuantity(vcpu=1.0, memory_mb=2048, disk_mb=8192),
                    count=4,
                    ttl_seconds=600,
                )

                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler-loop",
                            "--project",
                            "project-1",
                            "--state-dir",
                            raw_dir,
                            "--route-file",
                            str(route_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--no-private-network",
                            "--once",
                            "--execute",
                            "--output",
                            "json",
                        ]
                    )

                payload = json.loads(output.getvalue())
                remaining_demand = RoutingStore(route_file).pending_demand()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(payload["createdJobIds"], ["created-node"])
        self.assertEqual(payload["decision"]["preparedResources"]["vcpu"], 4.0)
        self.assertEqual(
            [item["prepare_id"] for item in payload["consumedPreparedCapacity"]],
            ["eval-soon"],
        )
        self.assertEqual(remaining_demand.prepared_resources, ResourceQuantity())

    def test_autoscaler_loop_once_uses_route_file_pending_image_builds(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            route_file = root / "routes.json"
            RoutingStore(route_file).upsert_pending_image_build(
                "custom",
                "registry.example.org/custom:latest",
            )

            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler-loop",
                        "--project",
                        "project-1",
                        "--state-dir",
                        raw_dir,
                        "--route-file",
                        str(route_file),
                        "--jobs-file",
                        str(jobs_file),
                        "--no-private-network",
                        "--once",
                        "--output",
                        "json",
                    ]
                )

            payload = json.loads(output.getvalue())

        self.assertEqual(result, 0)
        self.assertEqual(payload["pendingImageBuilds"], 1)
        self.assertEqual(payload["builderDecision"]["actions"][0]["kind"], "create")
        labels = payload["builderCreateIntents"][0]["payloadItem"]["labels"]
        self.assertEqual(labels["ucloud-sandboxes/builder"], "true")
        self.assertNotIn("ucloud-sandboxes/node", labels)

    def test_executing_autoscaler_loop_consumes_pending_image_build_signal(self) -> None:
        submitted: list[tuple[str, dict]] = []

        class FakeUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, project_id: str, payload: dict) -> dict:
                submitted.append((project_id, payload))
                return {"responses": [{"id": "created-builder"}]}

        original_client = cli.UCloudClient
        cli.UCloudClient = FakeUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                route_file = root / "routes.sqlite"
                RoutingStore(route_file).upsert_pending_image_build(
                    "custom",
                    "registry.example.org/custom:latest",
                )

                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler-loop",
                            "--project",
                            "project-1",
                            "--state-dir",
                            raw_dir,
                            "--route-file",
                            str(route_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--no-private-network",
                            "--once",
                            "--execute",
                            "--output",
                            "json",
                        ]
                    )

                payload = json.loads(output.getvalue())
                remaining_builds = RoutingStore(route_file).pending_image_build_count()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(payload["pendingImageBuilds"], 1)
        self.assertEqual(payload["createdJobIds"], ["created-builder"])
        self.assertEqual(
            [item["image_id"] for item in payload["consumedPendingImageBuilds"]],
            ["custom"],
        )
        self.assertEqual(payload["builderDecision"]["actions"][0]["kind"], "create")
        self.assertEqual(remaining_builds, 0)

    def test_executing_autoscaler_loop_consumes_prepared_builder_signal(self) -> None:
        submitted: list[tuple[str, dict]] = []

        class FakeUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, project_id: str, payload: dict) -> dict:
                submitted.append((project_id, payload))
                return {"responses": [{"id": "created-builder"}]}

        original_client = cli.UCloudClient
        cli.UCloudClient = FakeUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                route_file = root / "routes.sqlite"
                RoutingStore(route_file).upsert_prepared_builder(
                    "builds-soon",
                    count=2,
                    ttl_seconds=600,
                )

                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler-loop",
                            "--project",
                            "project-1",
                            "--state-dir",
                            raw_dir,
                            "--route-file",
                            str(route_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--no-private-network",
                            "--max-builder-nodes",
                            "2",
                            "--once",
                            "--execute",
                            "--output",
                            "json",
                        ]
                    )

                payload = json.loads(output.getvalue())
                remaining_builder_count = RoutingStore(route_file).prepared_builder_count()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(payload["createdJobIds"], ["created-builder"])
        self.assertEqual(payload["preparedBuilderCount"], 2)
        self.assertEqual(
            [item["prepare_id"] for item in payload["consumedPreparedBuilders"]],
            ["builds-soon"],
        )
        self.assertEqual(payload["builderDecision"]["actions"][0]["kind"], "create")
        labels = payload["builderCreateIntents"][0]["payloadItem"]["labels"]
        self.assertEqual(labels["ucloud-sandboxes/builder"], "true")
        self.assertEqual(remaining_builder_count, 0)

    def test_reconcile_plans_bootstrap_for_running_node_without_heartbeat(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "job-1",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-one",
                                    "hostname": "sandbox-node-one",
                                    "application": {"name": "vm-ubuntu", "version": "24.04"},
                                    "product": {
                                        "id": "cpu-amd-zen5-2-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                    },
                                    "parameters": {"diskSize": {"value": 50}},
                                },
                                "status": {
                                    "state": "RUNNING",
                                    "jobParametersJson": {
                                        "request": {
                                            "resolvedProduct": {
                                                "cpu": 2,
                                                "memoryInGigs": 6,
                                            },
                                        }
                                    },
                                },
                                "updates": [
                                    {
                                        "status": (
                                            "SSH Access: "
                                            "ssh ucloud@ssh.cloud.sdu.dk -p 41231"
                                        )
                                    }
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "reconcile",
                        "--project",
                        "project-1",
                        "--deployment-id",
                        "prod-a",
                        "--state-dir",
                        raw_dir,
                        "--jobs-file",
                        str(jobs_file),
                        "--no-private-network",
                        "--init-heartbeat-url",
                        "http://sandbox-gateway:8090/v1/nodes/heartbeat",
                        "--output",
                        "json",
                    ]
                )
            payload = json.loads(output.getvalue())

        self.assertEqual(result, 0)
        self.assertEqual(len(payload["bootstrapIntents"]), 1)
        intent = payload["bootstrapIntents"][0]
        self.assertTrue(intent["runnable"])
        self.assertEqual(intent["jobId"], "job-1")
        self.assertEqual(intent["nodeId"], "sandbox-node-one")
        self.assertEqual(intent["role"], "sandbox")
        self.assertEqual(intent["options"]["totalResources"]["vcpu"], 2.0)
        self.assertEqual(intent["options"]["totalResources"]["memory_mb"], 6144)

    def test_execute_init_runs_bootstrap_and_records_state(self) -> None:
        calls: list[dict] = []

        class FakeInitResult:
            returncode = 0

        def fake_run_init_over_ssh(
            ssh_command: str,
            script: str,
            *,
            timeout_seconds: int | None = None,
            private_key_file: str | None = None,
        ) -> FakeInitResult:
            calls.append(
                {
                    "ssh_command": ssh_command,
                    "script": script,
                    "timeout_seconds": timeout_seconds,
                    "private_key_file": private_key_file,
                }
            )
            return FakeInitResult()

        original = cli.run_init_over_ssh
        cli.run_init_over_ssh = fake_run_init_over_ssh
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                token_source = root / "gateway-token"
                token_source.write_text("SECRET", encoding="utf-8")
                state_file = root / "bootstrap.json"
                jobs_file = root / "jobs.json"
                jobs_file.write_text(
                    json.dumps(
                        {
                            "items": [
                                {
                                    "id": "job-1",
                                    "owner": {"project": "project-1"},
                                    "specification": {
                                        "name": "ucloud-sandbox-builder-one",
                                        "hostname": "sandbox-builder-one",
                                        "application": {
                                            "name": "vm-ubuntu",
                                            "version": "24.04",
                                        },
                                        "product": {
                                            "id": "cpu-amd-zen5-16-vcpu",
                                            "category": "cpu-amd-zen5",
                                        },
                                        "labels": {
                                            "ucloud-sandboxes/builder": "true",
                                            "ucloud-sandboxes/deployment": "prod-a",
                                        },
                                    },
                                    "status": {"state": "RUNNING"},
                                    "updates": [
                                        {
                                            "status": (
                                                "SSH Access: "
                                                "ssh ucloud@ssh.cloud.sdu.dk -p 41231"
                                            )
                                        }
                                    ],
                                }
                            ]
                        }
                    ),
                    encoding="utf-8",
                )

                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "reconcile",
                            "--project",
                            "project-1",
                            "--deployment-id",
                            "prod-a",
                            "--state-dir",
                            raw_dir,
                            "--jobs-file",
                            str(jobs_file),
                            "--no-private-network",
                            "--init-state-file",
                            str(state_file),
                            "--execute-init",
                            "--init-heartbeat-url",
                            "http://sandbox-gateway:8090/v1/nodes/heartbeat",
                            "--init-heartbeat-bearer-token-file",
                            "/work/ucloud-sandboxes/state/gateway-token",
                            "--init-heartbeat-bearer-token-source-file",
                            str(token_source),
                            "--init-ssh-private-key-file",
                            "/work/ucloud-sandboxes/state/ssh/gateway-init",
                            "--init-cpu-overcommit",
                            "2",
                            "--init-memory-overcommit",
                            "1.2",
                            "--init-docker-insecure-registry",
                            "ucloud-sandbox-registry:5000",
                            "--init-host-alias",
                            "ucloud-sandbox-registry=10.36.125.67",
                            "--output",
                            "json",
                        ]
                    )
                payload = json.loads(output.getvalue())
                state = json.loads(state_file.read_text(encoding="utf-8"))
        finally:
            cli.run_init_over_ssh = original

        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["private_key_file"], "/work/ucloud-sandboxes/state/ssh/gateway-init")
        self.assertIn(
            "UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON='[\"ucloud-sandbox-registry:5000\"]'",
            calls[0]["script"],
        )
        self.assertIn(
            "UCLOUD_HOST_ALIASES_JSON='[\"ucloud-sandbox-registry=10.36.125.67\"]'",
            calls[0]["script"],
        )
        self.assertIn("UCLOUD_CPU_OVERCOMMIT=1.0", calls[0]["script"])
        self.assertIn("UCLOUD_MEMORY_OVERCOMMIT=1.0", calls[0]["script"])
        self.assertIn("UCLOUD_DISK_OVERCOMMIT=1.0", calls[0]["script"])
        self.assertIn("UCLOUD_HEARTBEAT_BEARER_TOKEN=SECRET", calls[0]["script"])
        self.assertIn("--enable-image-builds --execute-runtime", calls[0]["script"])
        self.assertEqual(payload["bootstrapResults"][0]["status"], "succeeded")
        self.assertEqual(state["jobs"]["job-1"]["status"], "succeeded")
        self.assertEqual(state["jobs"]["job-1"]["attempts"], 1)

    def test_direct_builder_init_ignores_overcommit(self) -> None:
        options = cli.vm_init_options_from_args(
            argparse.Namespace(
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                heartbeat_bearer_token_file="",
                heartbeat_bearer_token_source_file=None,
                service_user="ucloud",
                init_authorized_key=[],
                init_authorized_key_file=[],
                node_id="builder-1",
                work_dir="/work/ucloud-sandboxes",
                package_spec="ucloud-sandboxes",
                node_agent_host="0.0.0.0",
                node_agent_port=8090,
                node_url="",
                agent_version="0.1.0",
                deployment_id="prod-a",
                init_version="2",
                ssh_port_start=22000,
                ssh_port_end=22999,
                total_vcpu=16,
                total_memory_mb=32768,
                total_disk_mb=204800,
                cpu_overcommit=2.0,
                memory_overcommit=1.2,
                disk_overcommit=1.5,
                docker_quota_image_gb=200,
                docker_insecure_registry=[],
                host_alias=[],
                enable_image_builds=True,
                runtime_dry_run=False,
                heartbeat_interval_seconds=20,
                label=[],
            ),
            "job-1",
        )

        self.assertEqual(options.cpu_overcommit, 1.0)
        self.assertEqual(options.memory_overcommit, 1.0)
        self.assertEqual(options.disk_overcommit, 1.0)

    def test_execute_stops_skips_blocked_jobs_without_failing(self) -> None:
        terminated: list[tuple[str, tuple[str, ...]]] = []

        class FakeUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def terminate_jobs(self, project_id: str, job_ids: tuple[str, ...]) -> dict:
                terminated.append((project_id, tuple(job_ids)))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        def job_payload(job_id: str, deployment_id: str) -> dict:
            return {
                "id": job_id,
                "owner": {"project": "project-1"},
                "specification": {
                    "name": f"ucloud-sandbox-node-{job_id}",
                    "application": {"name": "vm-ubuntu", "version": "24.04"},
                    "product": {
                        "id": "cpu-amd-zen5-2-vcpu",
                        "category": "cpu-amd-zen5",
                    },
                    "labels": {
                        "ucloud-sandboxes/node": "true",
                        "ucloud-sandboxes/deployment": deployment_id,
                    },
                    "parameters": {"diskSize": {"value": 50}},
                },
                "status": {
                    "state": "RUNNING",
                    "jobParametersJson": {
                        "request": {
                            "resolvedProduct": {"cpu": 2, "memoryInGigs": 6},
                        },
                    },
                },
            }

        original_client = cli.UCloudClient
        cli.UCloudClient = FakeUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text(
                    json.dumps(
                        {
                            "items": [
                                job_payload("owned", "prod-a"),
                                job_payload("foreign", "prod-b"),
                            ]
                        }
                    ),
                    encoding="utf-8",
                )
                heartbeat_file = root / "heartbeats.json"
                HeartbeatStore(heartbeat_file).save(
                    {
                        job_id: NodeHeartbeat(
                            node_id=f"node-{job_id}",
                            job_id=job_id,
                            updated_at=utc_now(),
                            active_sandboxes=0,
                            total_resources=ResourceQuantity(
                                vcpu=2.0,
                                memory_mb=6144,
                                disk_mb=51200,
                            ),
                            capabilities=("disk-quota",),
                        )
                        for job_id in ("owned", "foreign")
                    }
                )
                config = AutoscalerConfig(
                    project_id="project-1",
                    deployment_id="prod-a",
                    ucloud_session_file=str(root / "session.json"),
                    state_dir=raw_dir,
                    policy=ScalePolicy(max_stop_per_cycle=2, scale_down_idle_seconds=0),
                )
                args = argparse.Namespace(
                    jobs_file=jobs_file,
                    heartbeats=heartbeat_file,
                    include_job=["foreign"],
                    all_vm_jobs=False,
                    execute=False,
                    execute_stops=True,
                    allow_unlabeled_stops=False,
                    pending_image_builds=0,
                    max_builder_nodes=0,
                    seed_prefix="test",
                )

                result = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=cli.sandbox_demand_from_args(
                        argparse.Namespace(
                            pending_vcpu=0.0,
                            pending_memory_mb=0,
                            pending_disk_mb=0,
                            oldest_pending_seconds=0,
                        )
                    ),
                )
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(terminated, [("project-1", ("owned",))])
        self.assertEqual(result["stopJobIds"], ["owned"])
        self.assertEqual(result["blockedStopJobIds"], ["foreign"])

    def test_runtime_conformance_json_failure_returns_nonzero(self) -> None:
        original = cli.DockerRuntimeProbe
        cli.DockerRuntimeProbe = FailingProbe
        try:
            with redirect_stdout(io.StringIO()):
                result = cli.cmd_runtime_conformance(
                    argparse.Namespace(
                        docker_binary="docker",
                        runtime_name="runsc",
                        image="busybox",
                        sudo=False,
                        execute=True,
                        output="json",
                    )
                )
        finally:
            cli.DockerRuntimeProbe = original

        self.assertEqual(result, 1)

    def test_serve_node_agent_enables_tmpfs_workspace_from_conformance(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            conformance_file = root / "runtime-conformance.json"
            conformance_file.write_text(
                json.dumps(
                    {
                        "ok": True,
                        "results": [
                            {"name": "storage-opt-quota-enforced", "ok": True},
                            {"name": "tmpfs-quota-enforced", "ok": True},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            captured: dict = {}

            class FakeServer:
                server_address = ("127.0.0.1", 0)

                def serve_forever(self) -> None:
                    return None

                def server_close(self) -> None:
                    return None

            def fake_build_node_agent_server(*_args, **kwargs):
                captured.update(kwargs)
                return FakeServer()

            original = cli.build_node_agent_server
            cli.build_node_agent_server = fake_build_node_agent_server
            try:
                with redirect_stdout(io.StringIO()):
                    cli.cmd_serve_node_agent(
                        argparse.Namespace(
                            config=None,
                            session_file=None,
                            deployment_id=None,
                            state_dir=raw_dir,
                            sandbox_file=root / "sandboxes.json",
                            image_file=root / "images.json",
                            job_id="job-1",
                            node_id="node-1",
                            runtime_conformance_file=conformance_file,
                            docker_binary="docker",
                            runtime_name="runsc",
                            execute_runtime=True,
                            host="127.0.0.1",
                            port=0,
                            node_url="http://node-1:8090",
                            agent_version="0.1.0",
                            init_version="2",
                            total_vcpu=2.0,
                            total_memory_mb=4096,
                            total_disk_mb=10_000,
                            cpu_overcommit=1.0,
                            memory_overcommit=1.0,
                            disk_overcommit=1.0,
                            ssh_port_start=22000,
                            ssh_port_end=22999,
                            enable_image_builds=True,
                        )
                    )
            finally:
                cli.build_node_agent_server = original

        runtime = captured["runtime"]
        self.assertTrue(runtime.allow_storage_opt_quota)
        self.assertTrue(runtime.allow_tmpfs_workspace)


if __name__ == "__main__":
    unittest.main()
