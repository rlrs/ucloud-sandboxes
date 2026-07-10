import argparse
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes import cli
from ucloud_sandboxes.autoscaler_state import AutoscalerStateStore
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
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.images import ImageRecord, ImageStore
from ucloud_sandboxes.managed_registry import RegistryTag, RegistryUsageStore
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxDemand,
    ScalePolicy,
    VmJob,
    utc_now,
)
from ucloud_sandboxes.registry import HeartbeatStore
from ucloud_sandboxes.routing import RoutingState, RoutingStore, SandboxRoute
from ucloud_sandboxes.ucloud import UCloudError, UCloudHttpError


def allow_fixture_mutations(test):
    """Keep legacy provider-journal unit cases on deterministic fixtures."""

    @wraps(test)
    def wrapped(*args, **kwargs):
        with patch.object(cli, "reject_mutating_jobs_fixture", return_value=None):
            return test(*args, **kwargs)

    return wrapped


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
    def test_mutating_reconcile_commands_reject_jobs_fixture_before_provider_calls(
        self,
    ) -> None:
        class ForbiddenClient:
            def __init__(self, *_args, **_kwargs) -> None:
                raise AssertionError("provider client must not be constructed")

        with TemporaryDirectory() as raw_dir:
            jobs_file = Path(raw_dir) / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            for command in ("reconcile", "autoscaler-loop"):
                for mutation_flag in ("--execute", "--execute-stops", "--execute-init"):
                    with self.subTest(command=command, mutation_flag=mutation_flag):
                        argv = [
                            command,
                            "--project",
                            "project-1",
                            "--state-dir",
                            raw_dir,
                            "--jobs-file",
                            str(jobs_file),
                            mutation_flag,
                        ]
                        if command == "autoscaler-loop":
                            argv.append("--once")
                        stderr = io.StringIO()
                        with patch.object(cli, "UCloudClient", ForbiddenClient):
                            with redirect_stderr(stderr):
                                result = cli.main(argv)
                        self.assertEqual(result, 1)
                        expected = (
                            "reconcile is read-only"
                            if command == "reconcile"
                            else "--jobs-file is dry-run only"
                        )
                        self.assertIn(expected, stderr.getvalue())

    def test_autoscaler_provider_state_is_deployment_scoped_not_route_scoped(self) -> None:
        config = AutoscalerConfig(
            project_id="project-1",
            deployment_id="prod-a",
            state_dir="/var/lib/ucloud-sandboxes",
        )

        self.assertEqual(
            cli._autoscaler_state_path(config),
            Path("/var/lib/ucloud-sandboxes/autoscaler-state.sqlite"),
        )

    def test_registry_prune_cli_honors_active_image_lease(self) -> None:
        class FakeRegistryClient:
            deleted: list[tuple[str, str]] = []

            def __init__(self, _url: str) -> None:
                self.base_url = "http://registry.invalid"

            def catalog(self) -> list[str]:
                return ["repo/a"]

            def tags(self, _repository: str) -> list[str]:
                return ["v1"]

            def tag_record(self, repository: str, tag: str) -> RegistryTag:
                return RegistryTag(repository, tag, "sha256:one")

            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted.append((repository, digest))

        with TemporaryDirectory() as raw_dir:
            usage_file = Path(raw_dir) / "registry-usage.json"
            RegistryUsageStore(usage_file).acquire_lease(
                "repo/a",
                "v1",
                "sandbox:1:generation:2",
                ttl_seconds=60,
            )
            output = io.StringIO()
            FakeRegistryClient.deleted = []
            with patch.object(cli, "RegistryClient", FakeRegistryClient):
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "registry-prune",
                            "--registry-url",
                            "http://registry.invalid",
                            "--keep-per-repository",
                            "0",
                            "--usage-file",
                            str(usage_file),
                            "--execute",
                        ]
                    )
            payload = json.loads(output.getvalue())

        self.assertEqual(result, 0)
        self.assertEqual(FakeRegistryClient.deleted, [])
        self.assertEqual(payload["deleted"], [])
        self.assertEqual(payload["active_lease_count"], 1)

    def test_partial_scale_up_does_not_satisfy_larger_resource_deficit(self) -> None:
        results = [
            {
                "kind": "create",
                "role": "sandbox",
                "state": "applied",
                "jobIds": [f"job-{index}"],
            }
            for index in range(4)
        ]

        self.assertFalse(
            cli._sandbox_capacity_operation_succeeded(
                results,
                ResourceQuantity(vcpu=128, memory_mb=262144, disk_mb=524288),
                ResourceQuantity(vcpu=16, memory_mb=32768, disk_mb=204800),
            )
        )

    def test_provider_http_rejection_and_ambiguity_are_journaled_differently(
        self,
    ) -> None:
        class RejectingClient:
            def __init__(self, status: int) -> None:
                self.status = status

            def terminate_jobs(self, *_args, **_kwargs) -> dict:
                raise UCloudHttpError("POST", "/api/jobs/terminate", self.status, {})

        with TemporaryDirectory() as raw_dir:
            state = AutoscalerStateStore(Path(raw_dir) / "autoscaler-state.sqlite")
            definite_drain = state.prepare_drain_intent(
                deployment_id="prod-a",
                job_id="definite",
                role="sandbox",
            )
            definite = state.prepare_operation(
                intent_key="sandbox:definite",
                kind="stop",
                deployment_id="prod-a",
                role="sandbox",
                request={
                    "type": "bulk",
                    "items": [{"id": "definite"}],
                    "drainToken": definite_drain.token,
                    "drainReady": True,
                },
                target_job_ids=("definite",),
            )
            definite_result = cli.apply_prepared_provider_operations(
                state,
                RejectingClient(400),
                "project-1",
                source="test",
                allowed_kinds={"stop"},
                allowed_stop_operation_ids={definite.operation_id},
            )
            ambiguous_drain = state.prepare_drain_intent(
                deployment_id="prod-a",
                job_id="ambiguous",
                role="sandbox",
            )
            ambiguous = state.prepare_operation(
                intent_key="sandbox:ambiguous",
                kind="stop",
                deployment_id="prod-a",
                role="sandbox",
                request={
                    "type": "bulk",
                    "items": [{"id": "ambiguous"}],
                    "drainToken": ambiguous_drain.token,
                    "drainReady": True,
                },
                target_job_ids=("ambiguous",),
            )
            ambiguous_result = cli.apply_prepared_provider_operations(
                state,
                RejectingClient(503),
                "project-1",
                source="test",
                allowed_kinds={"stop"},
                allowed_stop_operation_ids={ambiguous.operation_id},
            )
            definite_state = state.get_operation(definite.operation_id).state
            ambiguous_state = state.get_operation(ambiguous.operation_id).state

        self.assertEqual(definite_result[0]["state"], "failed")
        self.assertEqual(definite_state, "failed")
        self.assertEqual(ambiguous_result[0]["state"], "uncertain")
        self.assertEqual(ambiguous_state, "uncertain")

    def test_control_plane_parser_accepts_distinct_heartbeat_token_file(self) -> None:
        args = cli.build_parser().parse_args(
            [
                "serve-control-plane",
                "--heartbeat-bearer-token-file",
                "/tmp/heartbeat-token",
            ]
        )

        self.assertEqual(
            args.heartbeat_bearer_token_file,
            Path("/tmp/heartbeat-token"),
        )

    def test_model_relay_cli_wires_admission_limits(self) -> None:
        args = cli.build_parser().parse_args(
            [
                "serve-model-relay",
                "--max-inflight-requests",
                "123",
                "--max-inflight-requests-per-rollout",
                "45",
                "--max-inflight-bytes",
                "6789",
            ]
        )

        with (
            patch.object(cli, "create_model_relay_app", return_value=object()) as create,
            patch("aiohttp.web.run_app") as run_app,
            redirect_stdout(io.StringIO()),
        ):
            result = cli.cmd_serve_model_relay(args)

        self.assertEqual(result, 0)
        self.assertEqual(create.call_args.kwargs["max_inflight_requests"], 123)
        self.assertEqual(
            create.call_args.kwargs["max_inflight_requests_per_rollout"], 45
        )
        self.assertEqual(create.call_args.kwargs["max_inflight_bytes"], 6789)
        run_app.assert_called_once()

    def test_model_relay_cli_rejects_nonpositive_admission_limit(self) -> None:
        args = cli.build_parser().parse_args(
            ["serve-model-relay", "--max-inflight-requests", "0"]
        )

        with self.assertRaisesRegex(ValueError, "max-inflight-requests must be positive"):
            cli.cmd_serve_model_relay(args)

    def test_top_level_version_flag_reports_package_version(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            with self.assertRaises(SystemExit) as raised:
                cli.main(["--version"])

        self.assertEqual(raised.exception.code, 0)
        self.assertEqual(
            output.getvalue().strip(), f"ucloud-sandboxes {package_version()}"
        )

    def test_remove_image_records_for_registry_tags_matches_full_image_refs(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            image_file = Path(raw_dir) / "images.json"
            now = utc_now()
            store = ImageStore(image_file)
            store.upsert(
                ImageRecord(
                    id="keep",
                    tag="ucloud-sandbox-registry:5000/prime-rl/keep:latest",
                    source="build:/tmp/keep",
                    state="available",
                    created_at=now,
                    updated_at=now,
                    pushed=True,
                )
            )
            store.upsert(
                ImageRecord(
                    id="delete",
                    tag="ucloud-sandbox-registry:5000/prime-rl/delete:latest",
                    source="build:/tmp/delete",
                    state="available",
                    created_at=now,
                    updated_at=now,
                    pushed=True,
                )
            )

            removed = cli._remove_image_records_for_registry_tags(
                image_file,
                {("prime-rl/delete", "latest")},
            )

            self.assertEqual([record.id for record in removed], ["delete"])
            self.assertEqual(list(store.load()), ["keep"])

    def test_remove_stale_private_build_image_records_keeps_external_tags(self) -> None:
        class FakeRegistryClient:
            base_url = "http://127.0.0.1:5000"

            def tag_exists(self, repository: str, tag: str) -> bool:
                return (repository, tag) != ("prime-rl/missing", "latest")

        with TemporaryDirectory() as raw_dir:
            image_file = Path(raw_dir) / "images.json"
            now = utc_now()
            store = ImageStore(image_file)
            store.upsert(
                ImageRecord(
                    id="missing",
                    tag="ucloud-sandbox-registry:5000/prime-rl/missing:latest",
                    source="build:/tmp/missing",
                    state="available",
                    created_at=now,
                    updated_at=now,
                    pushed=True,
                )
            )
            store.upsert(
                ImageRecord(
                    id="external",
                    tag="ghcr.io/prime-rl/missing:latest",
                    source="build:/tmp/external",
                    state="available",
                    created_at=now,
                    updated_at=now,
                    pushed=True,
                )
            )

            removed = cli._remove_stale_private_build_image_records(
                image_file,
                FakeRegistryClient(),  # type: ignore[arg-type]
            )

            self.assertEqual([record.id for record in removed], ["missing"])
            self.assertEqual(list(store.load()), ["external"])

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
        self.assertEqual(
            options.job_item()["labels"]["ucloud-sandboxes/gateway"], "true"
        )
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

        self.assertEqual(
            options.file_mounts[0].path, "/1234567/ucloud-sandbox-registry"
        )
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

            self.assertEqual(
                read_public_ssh_key_file(key_file), "ssh-ed25519 AAAA gateway"
            )

            key_file.write_text(
                "ssh-ed25519 AAAA gateway\nssh-ed25519 BBBB other\n", encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                read_public_ssh_key_file(key_file)

    def test_find_ucloud_ssh_key_matches_key_material(self) -> None:
        items = [
            {
                "id": "1",
                "specification": {"title": "other", "key": "ssh-ed25519 AAAA other"},
            },
            {
                "id": "2",
                "specification": {
                    "title": "gateway",
                    "key": "ssh-ed25519 BBBB gateway",
                },
            },
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

    def test_autoscaler_text_output_hides_final_pool_node_history(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "old-node",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-old",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-16-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                        "ucloud-sandboxes/agent-version": package_version(),
                                    },
                                    "parameters": {"diskSize": {"value": 250}},
                                },
                                "status": {
                                    "state": "SUCCESS",
                                    "jobParametersJson": {
                                        "request": {
                                            "resolvedProduct": {
                                                "cpu": 16,
                                                "memoryInGigs": 32,
                                            },
                                        },
                                    },
                                },
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
                        "autoscaler-loop",
                        "--project",
                        "project-1",
                        "--deployment-id",
                        "prod-a",
                        "--state-dir",
                        raw_dir,
                        "--jobs-file",
                        str(jobs_file),
                        "--no-private-network",
                        "--once",
                        "--output",
                        "text",
                    ]
                )

        self.assertEqual(result, 0)
        text = output.getvalue()
        self.assertIn("Nodes: 0 ready, 0 provisioning, 0 total", text)
        self.assertIn("No pool nodes matched the configured selection.", text)
        self.assertNotIn("job=old-node", text)
        self.assertNotIn("state=SUCCESS", text)

    def test_autoscaler_prunes_routes_for_final_jobs(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "old-node",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-old",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-16-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                    },
                                    "parameters": {"diskSize": {"value": 250}},
                                },
                                "status": {"state": "SUCCESS"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "heartbeats.json"
            HeartbeatStore(heartbeat_file).save(
                {
                    "old-node": NodeHeartbeat(
                        node_id="node-old",
                        job_id="old-node",
                        updated_at=utc_now(),
                        active_sandboxes=1,
                        node_url="http://node-old:8090",
                    )
                }
            )
            route_file = root / "routes.sqlite"
            RoutingStore(route_file).upsert_sandbox(
                SandboxRoute(
                    sandbox_id="stale-sandbox",
                    node_id="node-old",
                    job_id="old-node",
                    node_url="http://node-old:8090",
                    resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                )
            )

            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler-loop",
                        "--project",
                        "project-1",
                        "--deployment-id",
                        "prod-a",
                        "--state-dir",
                        raw_dir,
                        "--route-file",
                        str(route_file),
                        "--heartbeats",
                        str(heartbeat_file),
                        "--jobs-file",
                        str(jobs_file),
                        "--no-private-network",
                        "--once",
                        "--output",
                        "json",
                    ]
                )
            payload = json.loads(output.getvalue())
            routes = RoutingStore(route_file).load().sandboxes

        self.assertEqual(result, 0)
        self.assertEqual(payload["prunedFinalHeartbeats"], ["old-node"])
        self.assertEqual(
            [route["sandbox_id"] for route in payload["removedRoutes"]],
            ["stale-sandbox"],
        )
        self.assertEqual(routes, {})

    @allow_fixture_mutations
    def test_autoscaler_prunes_orphaned_stale_routes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            route_file = root / "routes.sqlite"
            now = utc_now()
            old = (now - timedelta(seconds=600)).isoformat()
            recent = (now - timedelta(seconds=30)).isoformat()
            RoutingStore(route_file).save(
                RoutingState(
                    sandboxes={
                        "old-orphan": SandboxRoute(
                            sandbox_id="old-orphan",
                            node_id="old-node",
                            job_id="old-job",
                            node_url="http://old-node:8090",
                            created_at=old,
                            updated_at=old,
                        ),
                        "recent-orphan": SandboxRoute(
                            sandbox_id="recent-orphan",
                            node_id="recent-node",
                            job_id="recent-job",
                            node_url="http://recent-node:8090",
                            created_at=recent,
                            updated_at=recent,
                        ),
                    },
                    exec_sessions={},
                    pending={},
                    image_builds={},
                )
            )

            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler-loop",
                        "--project",
                        "project-1",
                        "--deployment-id",
                        "prod-a",
                        "--state-dir",
                        raw_dir,
                        "--route-file",
                        str(route_file),
                        "--jobs-file",
                        str(jobs_file),
                        "--no-private-network",
                        "--execute",
                        "--once",
                        "--output",
                        "json",
                    ]
                )
            payload = json.loads(output.getvalue())
            routes = RoutingStore(route_file).load().sandboxes

        self.assertEqual(result, 0)
        self.assertEqual(
            [route["sandbox_id"] for route in payload["removedRoutes"]],
            ["old-orphan"],
        )
        self.assertNotIn("old-orphan", routes)
        self.assertIn("recent-orphan", routes)

    def test_deploy_all_in_one_dry_run_outputs_plan_without_ucloud_lookup(self) -> None:
        with TemporaryDirectory() as raw_dir:
            wheel = Path(raw_dir) / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
            wheel.write_bytes(b"wheel")
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "deploy-all-in-one",
                        "job-1",
                        "--project",
                        "project-1",
                        "--deployment-id",
                        "prod-a",
                        "--private-network-id",
                        "net-1",
                        "--gateway-private-host",
                        "sandbox-gateway-prod",
                        "--registry-private-ip",
                        "10.0.0.5",
                        "--ssh-command",
                        "ssh ucloud@example.org -p 2222",
                        "--wheel",
                        str(wheel),
                        "--output",
                        "json",
                    ]
                )

            payload = json.loads(output.getvalue())

        self.assertEqual(result, 0)
        self.assertFalse(payload["execute"])
        self.assertEqual(payload["plan"]["deploymentId"], "prod-a")
        self.assertEqual(
            payload["plan"]["initHeartbeatUrl"],
            "http://sandbox-gateway-prod:8090/v1/nodes/heartbeat",
        )
        self.assertEqual(
            payload["plan"]["dockerHostAlias"], "ucloud-sandbox-registry=10.0.0.5"
        )

    def test_deploy_all_in_one_does_not_infer_registry_from_ucloud_job_label(
        self,
    ) -> None:
        class FailingUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def retrieve_job(self, *_args, **_kwargs) -> dict:
                raise AssertionError("UCloud lookup should not be needed")

        original_client = cli.UCloudClient
        cli.UCloudClient = FailingUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                wheel = Path(raw_dir) / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
                wheel.write_bytes(b"wheel")
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "deploy-all-in-one",
                            "job-1",
                            "--project",
                            "project-1",
                            "--deployment-id",
                            "prod-a",
                            "--private-network-id",
                            "net-1",
                            "--gateway-private-host",
                            "sandbox-gateway-prod",
                            "--ssh-command",
                            "ssh ucloud@example.org -p 2222",
                            "--wheel",
                            str(wheel),
                            "--output",
                            "json",
                        ]
                    )
        finally:
            cli.UCloudClient = original_client

        payload = json.loads(output.getvalue())
        self.assertEqual(result, 0)
        self.assertEqual(payload["plan"]["registryPrivateIp"], "")
        self.assertEqual(
            payload["plan"]["dockerHostAlias"],
            "ucloud-sandbox-registry=__UCLOUD_REGISTRY_PRIVATE_IP__",
        )

    @allow_fixture_mutations
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

    @allow_fixture_mutations
    def test_one_shot_autoscaler_refuses_competing_process_lock(self) -> None:
        class FailingUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, *_args, **_kwargs) -> dict:
                raise AssertionError("follower autoscaler must not submit")

        original_client = cli.UCloudClient
        cli.UCloudClient = FailingUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                route_file = root / "routes.sqlite"
                RoutingStore(route_file).upsert_pending(
                    "pending-one",
                    ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                )
                state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
                held = state.process_lock()
                self.assertTrue(held.acquire())

                output = io.StringIO()
                stderr = io.StringIO()
                try:
                    with redirect_stdout(output), redirect_stderr(stderr):
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
                finally:
                    held.release()
                remaining = RoutingStore(route_file).pending_demand()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 1)
        self.assertIn("controller lock", stderr.getvalue())
        self.assertEqual(remaining.pending_resources.vcpu, 1)

    @allow_fixture_mutations
    def test_reconcile_rejects_provider_mutation_flags(self) -> None:
        class FailingUCloudClient:
            def __init__(self, _session_store) -> None:
                raise AssertionError("read-only reconcile must not construct a provider client")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            stderr = io.StringIO()
            with patch.object(cli, "UCloudClient", FailingUCloudClient):
                with redirect_stderr(stderr):
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
                            "--pending-vcpu",
                            "1",
                            "--no-private-network",
                            "--execute",
                            "--output",
                            "json",
                        ]
                    )
        self.assertEqual(result, 1)
        self.assertIn("reconcile is read-only", stderr.getvalue())

    @allow_fixture_mutations
    def test_autoscaler_once_recovers_journaled_uncertain_create(self) -> None:
        submitted: list[dict] = []

        class AmbiguousCreateClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, _project_id: str, payload: dict) -> dict:
                submitted.append(payload)
                if len(submitted) > 1:
                    raise AssertionError("ambiguous create must not be resubmitted")
                raise UCloudError("connection dropped after submit")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            RoutingStore(root / "routes.sqlite").upsert_pending(
                "pending-one",
                ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
            )
            command = [
                "autoscaler-loop",
                "--project",
                "project-1",
                "--deployment-id",
                "prod-a",
                "--state-dir",
                raw_dir,
                "--jobs-file",
                str(jobs_file),
                "--seed-prefix",
                "one-shot",
                "--no-private-network",
                "--execute",
                "--once",
                "--output",
                "json",
            ]
            with patch.object(cli, "UCloudClient", AmbiguousCreateClient):
                first_output = io.StringIO()
                with redirect_stdout(first_output):
                    first_result = cli.main(command)
                first = json.loads(first_output.getvalue())

                jobs_file.write_text(
                    json.dumps(
                        {
                            "items": [
                                {
                                    "id": "recovered-job",
                                    "owner": {"project": "project-1"},
                                    "specification": submitted[0]["items"][0],
                                    "status": {"state": "IN_QUEUE"},
                                }
                            ]
                        }
                    ),
                    encoding="utf-8",
                )
                second_output = io.StringIO()
                with redirect_stdout(second_output):
                    second_result = cli.main(command)
                second = json.loads(second_output.getvalue())

            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
            operations = state.list_operations(kind="create")

        self.assertEqual(first_result, 0)
        self.assertEqual(first["providerOperationResults"][0]["state"], "uncertain")
        self.assertEqual(second_result, 0)
        self.assertEqual(len(submitted), 1)
        self.assertEqual(second["createRecoveryResults"][0]["state"], "recovered")
        self.assertEqual(second["createRecoveryResults"][0]["jobIds"], ["recovered-job"])
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0].state, "settled")
        self.assertTrue(first["autoscalerStateFile"].endswith("autoscaler-state.sqlite"))
        self.assertTrue(first["controllerLockHeld"])

    @allow_fixture_mutations
    def test_autoscaler_once_stop_requires_second_drain_invocation(self) -> None:
        terminate_calls: list[tuple[str, ...]] = []
        drain_tokens: list[str] = []

        class SuccessfulStopClient:
            def __init__(self, _session_store) -> None:
                pass

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                terminate_calls.append(tuple(job_ids))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        def post_drain(_node_url: str, token: str) -> dict:
            drain_tokens.append(token)
            return {"draining": True, "token": token}

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "owned",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-owned",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-2-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                        "ucloud-sandboxes/agent-version": package_version(),
                                    },
                                },
                                "status": {"state": "RUNNING"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "heartbeats.json"
            HeartbeatStore(heartbeat_file).save(
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        updated_at=utc_now(),
                        active_sandboxes=0,
                        idle_since=utc_now() - timedelta(minutes=10),
                        node_url="http://node-owned:8090",
                        agent_version=package_version(),
                        deployment_id="prod-a",
                        capabilities=("disk-quota",),
                        total_resources=ResourceQuantity(
                            vcpu=2,
                            memory_mb=6144,
                            disk_mb=51200,
                        ),
                    )
                }
            )
            command = [
                "autoscaler-loop",
                "--project",
                "project-1",
                "--deployment-id",
                "prod-a",
                "--state-dir",
                raw_dir,
                "--jobs-file",
                str(jobs_file),
                "--heartbeats",
                str(heartbeat_file),
                "--scale-down-idle-seconds",
                "0",
                "--max-builder-nodes",
                "0",
                "--execute-stops",
                "--once",
                "--output",
                "json",
            ]
            with patch.object(cli, "UCloudClient", SuccessfulStopClient), patch.object(
                cli, "_post_node_drain", side_effect=post_drain
            ):
                first_output = io.StringIO()
                with redirect_stdout(first_output):
                    first_result = cli.main(command)
                first = json.loads(first_output.getvalue())
                state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
                intent = state.list_drain_intents(state="active")[0]

                HeartbeatStore(heartbeat_file).save(
                    {
                        "owned": NodeHeartbeat(
                            node_id="node-owned",
                            job_id="owned",
                            updated_at=utc_now(),
                            active_sandboxes=0,
                            idle_since=utc_now() - timedelta(minutes=10),
                            node_url="http://node-owned:8090",
                            agent_version=package_version(),
                            deployment_id="prod-a",
                            capabilities=("disk-quota",),
                            total_resources=ResourceQuantity(
                                vcpu=2,
                                memory_mb=6144,
                                disk_mb=51200,
                            ),
                            draining=True,
                            admission_open=False,
                            drain_token=intent.token,
                            inventory_complete=True,
                            activity_epoch=7,
                            drain_activity_epoch=7,
                        )
                    }
                )
                second_output = io.StringIO()
                with redirect_stdout(second_output):
                    second_result = cli.main(command)
                second = json.loads(second_output.getvalue())

            stop_operations = state.list_operations(kind="stop")

        self.assertEqual(first_result, 0)
        self.assertEqual(first["definitelyTerminatedJobIds"], [])
        self.assertEqual(first["drainReadyStopJobIds"], [])
        self.assertEqual(terminate_calls, [("owned",)])
        self.assertEqual(second_result, 0)
        self.assertEqual(second["drainReadyStopJobIds"], ["owned"])
        self.assertEqual(second["definitelyTerminatedJobIds"], ["owned"])
        self.assertEqual(len(set(drain_tokens)), 1)
        self.assertEqual(len(stop_operations), 1)
        self.assertEqual(stop_operations[0].state, "accepted")

    @allow_fixture_mutations
    def test_autoscaler_loop_preserves_pending_signal_created_during_cycle(
        self,
    ) -> None:
        submitted: list[tuple[str, dict]] = []

        original_client = cli.UCloudClient
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

                class FakeUCloudClient:
                    def __init__(self, _session_store) -> None:
                        pass

                    def submit_jobs(self, project_id: str, payload: dict) -> dict:
                        submitted.append((project_id, payload))
                        RoutingStore(route_file).upsert_pending(
                            "pending-two",
                            ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
                        )
                        return {"responses": [{"id": "created-node"}]}

                cli.UCloudClient = FakeUCloudClient
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
                remaining = RoutingStore(route_file).pending_sandboxes()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(
            [item["sandbox_id"] for item in payload["consumedPendingDemand"]],
            ["pending-one"],
        )
        self.assertEqual([item.sandbox_id for item in remaining], ["pending-two"])

    @allow_fixture_mutations
    def test_ambiguous_create_recovers_before_planning_and_then_consumes_demand(
        self,
    ) -> None:
        submitted: list[dict] = []

        class AmbiguousUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, _project_id: str, payload: dict) -> dict:
                submitted.append(payload)
                raise UCloudError("connection dropped after submit")

        original_client = cli.UCloudClient
        cli.UCloudClient = AmbiguousUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                route_file = root / "routes.sqlite"
                RoutingStore(route_file).upsert_pending(
                    "pending-one",
                    ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                )
                command = [
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
                first_output = io.StringIO()
                with redirect_stdout(first_output):
                    first_result = cli.main(command)
                first = json.loads(first_output.getvalue())
                demand_after_ambiguity = RoutingStore(route_file).pending_demand()

                submitted_item = submitted[0]["items"][0]
                jobs_file.write_text(
                    json.dumps(
                        {
                            "items": [
                                {
                                    "id": "recovered-job",
                                    "owner": {"project": "project-1"},
                                    "specification": submitted_item,
                                    "status": {"state": "IN_QUEUE"},
                                }
                            ]
                        }
                    ),
                    encoding="utf-8",
                )
                second_output = io.StringIO()
                with redirect_stdout(second_output):
                    second_result = cli.main(command)
                second = json.loads(second_output.getvalue())
                demand_after_recovery = RoutingStore(route_file).pending_demand()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(first_result, 0)
        self.assertEqual(first["providerOperationResults"][0]["state"], "uncertain")
        self.assertEqual(first["consumedPendingDemand"], [])
        self.assertEqual(demand_after_ambiguity.pending_resources.vcpu, 1)
        self.assertEqual(second_result, 0)
        self.assertEqual(len(submitted), 1)
        self.assertEqual(second["createRecoveryResults"][0]["state"], "recovered")
        self.assertEqual(
            [item["sandbox_id"] for item in second["consumedPendingDemand"]],
            ["pending-one"],
        )
        self.assertEqual(demand_after_recovery.pending_resources, ResourceQuantity())

    @allow_fixture_mutations
    def test_applied_create_blocks_replacement_until_job_is_visible(self) -> None:
        submitted: list[dict] = []

        class SuccessfulUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, _project_id: str, payload: dict) -> dict:
                submitted.append(payload)
                if len(submitted) > 2:
                    raise AssertionError(
                        "settled create should allocate only one replacement"
                    )
                return {
                    "responses": [
                        {
                            "id": (
                                "delayed-job"
                                if len(submitted) == 1
                                else "replacement-job"
                            )
                        }
                    ]
                }

        original_client = cli.UCloudClient
        cli.UCloudClient = SuccessfulUCloudClient
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                jobs_file = root / "jobs.json"
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                route_file = root / "routes.sqlite"
                RoutingStore(route_file).upsert_pending(
                    "pending-one",
                    ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                )
                command = [
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
                with redirect_stdout(io.StringIO()):
                    first_result = cli.main(command)
                RoutingStore(route_file).upsert_pending(
                    "pending-two",
                    ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                )
                second_output = io.StringIO()
                with redirect_stdout(second_output):
                    second_result = cli.main(command)
                second = json.loads(second_output.getvalue())

                jobs_file.write_text(
                    json.dumps(
                        {
                            "items": [
                                {
                                    "id": "delayed-job",
                                    "owner": {"project": "project-1"},
                                    "specification": submitted[0]["items"][0],
                                    "status": {"state": "IN_QUEUE"},
                                }
                            ]
                        }
                    ),
                    encoding="utf-8",
                )
                third_output = io.StringIO()
                with redirect_stdout(third_output):
                    third_result = cli.main(command)
                third = json.loads(third_output.getvalue())

                # Provider history may later omit the completed/aged-out job.
                # Its already-observed operation must not block this slot forever.
                jobs_file.write_text('{"items": []}', encoding="utf-8")
                fourth_output = io.StringIO()
                with redirect_stdout(fourth_output):
                    fourth_result = cli.main(command)
                fourth = json.loads(fourth_output.getvalue())
                remaining = RoutingStore(route_file).pending_sandboxes()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(first_result, 0)
        self.assertEqual(second_result, 0)
        self.assertEqual(third_result, 0)
        self.assertEqual(fourth_result, 0)
        self.assertEqual(len(submitted), 2)
        self.assertEqual(second["blockedCreateRoles"], ["sandbox"])
        self.assertEqual(
            second["createVisibilityGuards"][0]["missingJobIds"],
            ["delayed-job"],
        )
        self.assertEqual(third["blockedCreateRoles"], [])
        self.assertEqual(fourth["blockedCreateRoles"], [])
        self.assertEqual(fourth["createdJobIds"], ["replacement-job"])
        self.assertEqual(remaining, [])

    @allow_fixture_mutations
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
        self.assertEqual(payload["activeImageBuilds"], 0)
        self.assertEqual(
            payload["buildWarmSandboxResources"],
            {"vcpu": 16.0, "memory_mb": 32768, "disk_mb": 204800},
        )
        self.assertEqual(payload["decision"]["actions"][0]["kind"], "create")
        self.assertEqual(payload["builderDecision"]["actions"][0]["kind"], "create")
        sandbox_labels = payload["sandboxCreateIntents"][0]["payloadItem"]["labels"]
        self.assertEqual(sandbox_labels["ucloud-sandboxes/node"], "true")
        labels = payload["builderCreateIntents"][0]["payloadItem"]["labels"]
        self.assertEqual(labels["ucloud-sandboxes/builder"], "true")
        self.assertNotIn("ucloud-sandboxes/node", labels)

    @allow_fixture_mutations
    def test_executing_autoscaler_loop_consumes_pending_image_build_signal(
        self,
    ) -> None:
        submitted: list[tuple[str, dict]] = []
        submitted_count = 0

        class FakeUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, project_id: str, payload: dict) -> dict:
                nonlocal submitted_count
                submitted.append((project_id, payload))
                submitted_count += 1
                return {"responses": [{"id": f"created-{submitted_count}"}]}

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
        self.assertEqual(payload["createdJobIds"], ["created-1", "created-2"])
        self.assertEqual(len(submitted), 2)
        self.assertTrue(all(len(call[1]["items"]) == 1 for call in submitted))
        self.assertTrue(
            all(
                "ucloud-sandboxes/provider-operation" in call[1]["items"][0]["labels"]
                for call in submitted
            )
        )
        self.assertEqual(
            [item["image_id"] for item in payload["consumedPendingImageBuilds"]],
            ["custom"],
        )
        self.assertEqual(payload["decision"]["actions"][0]["kind"], "create")
        self.assertEqual(payload["builderDecision"]["actions"][0]["kind"], "create")
        self.assertEqual(remaining_builds, 0)

    def test_build_activity_adds_transient_sandbox_warm_resources(self) -> None:
        policy = ScalePolicy(
            default_node_resources=ResourceQuantity(
                vcpu=8,
                memory_mb=16384,
                disk_mb=102400,
            )
        )

        resources = cli.build_activity_sandbox_warm_resources(
            active_image_builds=1,
            pending_image_builds=0,
            prepared_builder_count=0,
            policy=policy,
        )
        demand = cli.demand_with_build_warm_resources(SandboxDemand(), resources)

        self.assertEqual(resources, policy.default_node_resources)
        self.assertEqual(demand.prepared_resources, policy.default_node_resources)

    def test_no_build_activity_leaves_sandbox_demand_unchanged(self) -> None:
        demand = SandboxDemand(
            pending_resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)
        )
        resources = cli.build_activity_sandbox_warm_resources(
            active_image_builds=0,
            pending_image_builds=0,
            prepared_builder_count=0,
            policy=ScalePolicy(),
        )

        self.assertEqual(resources, ResourceQuantity())
        self.assertIs(cli.demand_with_build_warm_resources(demand, resources), demand)

    @allow_fixture_mutations
    def test_executing_autoscaler_loop_consumes_prepared_builder_signal(self) -> None:
        submitted: list[tuple[str, dict]] = []
        submitted_count = 0

        class FakeUCloudClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, project_id: str, payload: dict) -> dict:
                nonlocal submitted_count
                submitted.append((project_id, payload))
                submitted_count += 1
                return {"responses": [{"id": f"created-{submitted_count}"}]}

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
                remaining_builder_count = RoutingStore(
                    route_file
                ).prepared_builder_count()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(
            payload["createdJobIds"],
            ["created-1", "created-2", "created-3"],
        )
        self.assertEqual(len(submitted), 3)
        self.assertTrue(all(len(call[1]["items"]) == 1 for call in submitted))
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
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
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

    @allow_fixture_mutations
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
                            "autoscaler-loop",
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
                            "--once",
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
        self.assertEqual(
            calls[0]["private_key_file"],
            "/work/ucloud-sandboxes/state/ssh/gateway-init",
        )
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

    @allow_fixture_mutations
    def test_execute_init_runs_bootstraps_concurrently_with_isolated_results(
        self,
    ) -> None:
        barrier = threading.Barrier(3)
        active_lock = threading.Lock()
        active = 0
        peak_active = 0

        class FakeInitResult:
            def __init__(self, returncode: int) -> None:
                self.returncode = returncode

        def fake_run_init_over_ssh(
            ssh_command: str,
            _script: str,
            *,
            timeout_seconds: int | None = None,
            private_key_file: str | None = None,
        ) -> FakeInitResult:
            del timeout_seconds, private_key_file
            nonlocal active, peak_active
            with active_lock:
                active += 1
                peak_active = max(peak_active, active)
            try:
                barrier.wait(timeout=2)
                port = int(ssh_command.rsplit(" ", 1)[-1])
                # Complete in a different order than the input inventory.
                time.sleep({41231: 0.03, 41232: 0.01, 41233: 0.02}[port])
                return FakeInitResult(17 if port == 41232 else 0)
            finally:
                with active_lock:
                    active -= 1

        def raw_job(index: int) -> dict:
            return {
                "id": f"job-{index}",
                "owner": {"project": "project-1"},
                "specification": {
                    "name": f"ucloud-sandbox-node-{index}",
                    "hostname": f"sandbox-node-{index}",
                    "application": {"name": "vm-ubuntu", "version": "24.04"},
                    "product": {
                        "id": "cpu-amd-zen5-2-vcpu",
                        "category": "cpu-amd-zen5",
                    },
                    "labels": {
                        "ucloud-sandboxes/node": "true",
                        "ucloud-sandboxes/deployment": "prod-a",
                    },
                },
                "status": {"state": "RUNNING"},
                "updates": [
                    {
                        "status": (
                            "SSH Access: ssh ucloud@ssh.cloud.sdu.dk "
                            f"-p {41230 + index}"
                        )
                    }
                ],
            }

        original = cli.run_init_over_ssh
        cli.run_init_over_ssh = fake_run_init_over_ssh
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                state_file = root / "bootstrap.json"
                jobs_file = root / "jobs.json"
                jobs_file.write_text(
                    json.dumps({"items": [raw_job(index) for index in range(1, 4)]}),
                    encoding="utf-8",
                )

                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler-loop",
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
                            "--max-init-per-cycle",
                            "3",
                            "--once",
                            "--init-heartbeat-url",
                            "http://sandbox-gateway:8090/v1/nodes/heartbeat",
                            "--output",
                            "json",
                        ]
                    )
                payload = json.loads(output.getvalue())
                state = json.loads(state_file.read_text(encoding="utf-8"))
                metric_events = [
                    json.loads(line)
                    for line in (root / "metrics.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                ]
        finally:
            cli.run_init_over_ssh = original

        self.assertEqual(result, 0)
        self.assertEqual(peak_active, 3)
        self.assertEqual(
            [item["jobId"] for item in payload["bootstrapResults"]],
            ["job-1", "job-2", "job-3"],
        )
        self.assertEqual(
            [item["status"] for item in payload["bootstrapResults"]],
            ["succeeded", "failed", "succeeded"],
        )
        self.assertEqual(payload["bootstrapResults"][1]["returncode"], 17)
        self.assertEqual(
            [state["jobs"][f"job-{index}"]["status"] for index in range(1, 4)],
            ["succeeded", "failed", "succeeded"],
        )
        self.assertTrue(
            all(
                state["jobs"][f"job-{index}"]["attempts"] == 1
                for index in range(1, 4)
            )
        )
        init_metrics = [
            event for event in metric_events if event["kind"] == "vm_init_attempt"
        ]
        self.assertEqual(len(init_metrics), 3)
        self.assertEqual(
            {event["data"]["job_id"]: event["data"]["status"] for event in init_metrics},
            {"job-1": "succeeded", "job-2": "failed", "job-3": "succeeded"},
        )

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

    def test_unfenced_execute_stops_fails_closed(self) -> None:
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

                with self.assertRaisesRegex(
                    cli.AutoscalerStateError,
                    "require the local autoscaler controller lock",
                ):
                    cli.run_reconcile_cycle(
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
                remaining_heartbeats = HeartbeatStore(heartbeat_file).load()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(terminated, [])
        self.assertIn("owned", remaining_heartbeats)
        self.assertIn("foreign", remaining_heartbeats)

    def test_fenced_stop_waits_for_matching_empty_gateway_heartbeat(self) -> None:
        terminate_calls: list[tuple[str, ...]] = []
        drain_tokens: list[str] = []

        class SuccessfulStopClient:
            def __init__(self, _session_store) -> None:
                pass

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                terminate_calls.append(tuple(job_ids))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "owned",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-owned",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-2-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                    },
                                },
                                "status": {"state": "RUNNING"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "heartbeats.json"

            def save_heartbeat(
                *,
                token: str = "",
                updated_at=None,
                reserved: ResourceQuantity = ResourceQuantity(),
            ) -> None:
                HeartbeatStore(heartbeat_file).save(
                    {
                        "owned": NodeHeartbeat(
                            node_id="node-owned",
                            job_id="owned",
                            updated_at=updated_at or utc_now(),
                            active_sandboxes=0,
                            idle_since=utc_now() - timedelta(minutes=10),
                            node_url="http://node-owned:8090",
                            agent_version=package_version(),
                            capabilities=("disk-quota",),
                            draining=bool(token),
                            admission_open=not bool(token),
                            drain_token=token,
                            activity_epoch=7,
                            drain_activity_epoch=7 if token else 0,
                            inventory_complete=bool(token),
                            reserved_resources=reserved,
                        )
                    }
                )

            save_heartbeat()
            config = AutoscalerConfig(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                state_dir=raw_dir,
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                heartbeats=heartbeat_file,
                include_job=[],
                all_vm_jobs=False,
                execute=False,
                execute_stops=True,
                execute_init=False,
                allow_unlabeled_stops=False,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            def post_drain(_url: str, token: str) -> dict:
                drain_tokens.append(token)
                if len(drain_tokens) == 1:
                    raise TimeoutError("drain request timed out")
                return {"draining": True}

            with patch.object(cli, "UCloudClient", SuccessfulStopClient), patch.object(
                cli, "_post_node_drain", side_effect=post_drain
            ):
                failed_request = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                intent = state.list_drain_intents(state="active")[0]

                save_heartbeat(
                    token=intent.token,
                    updated_at=utc_now() - timedelta(hours=1),
                )
                stale = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                save_heartbeat(token="wrong-token")
                mismatch = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                save_heartbeat(
                    token=intent.token,
                    reserved=ResourceQuantity(vcpu=1),
                )
                reserved = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                save_heartbeat(token=intent.token)
                acknowledged = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )

        self.assertEqual(terminate_calls, [("owned",)])
        self.assertEqual(len(set(drain_tokens)), 1)
        for blocked in (failed_request, stale, mismatch, reserved):
            self.assertEqual(blocked["definitelyTerminatedJobIds"], [])
        self.assertEqual(acknowledged["drainReadyStopJobIds"], ["owned"])
        self.assertEqual(acknowledged["definitelyTerminatedJobIds"], ["owned"])
        self.assertEqual(mismatch["stopJobIds"], ["owned"])
        self.assertEqual(mismatch["drainingJobIds"], ["owned"])

    def test_demand_rise_durably_cancels_drain_before_ambiguous_undrain(self) -> None:
        terminate_calls: list[tuple[str, ...]] = []
        drain_actions: list[tuple[str, bool]] = []

        class SuccessfulStopClient:
            def __init__(self, _session_store) -> None:
                pass

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                terminate_calls.append(tuple(job_ids))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "owned",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-owned",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-2-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                    },
                                },
                                "status": {"state": "RUNNING"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "heartbeats.json"
            HeartbeatStore(heartbeat_file).save(
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        updated_at=utc_now(),
                        active_sandboxes=0,
                        idle_since=utc_now() - timedelta(minutes=10),
                        node_url="http://node-owned:8090",
                        agent_version=package_version(),
                        capabilities=("disk-quota",),
                        total_resources=ResourceQuantity(
                            vcpu=2,
                            memory_mb=6144,
                            disk_mb=51200,
                        ),
                    )
                }
            )
            config = AutoscalerConfig(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                state_dir=raw_dir,
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                heartbeats=heartbeat_file,
                include_job=[],
                all_vm_jobs=False,
                execute=False,
                execute_stops=True,
                execute_init=False,
                allow_unlabeled_stops=False,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            cancel_attempts = 0

            def post_drain(
                _url: str,
                token: str,
                *,
                draining: bool = True,
                bearer_token: str | None = None,
            ) -> dict:
                del bearer_token
                nonlocal cancel_attempts
                drain_actions.append((token, draining))
                if not draining:
                    cancel_attempts += 1
                    if cancel_attempts == 1:
                        raise TimeoutError("undrain response lost")
                return {
                    "drain": {
                        "token": token,
                        "draining": draining,
                        "admission_open": not draining,
                    }
                }

            with patch.object(cli, "UCloudClient", SuccessfulStopClient), patch.object(
                cli, "_post_node_drain", side_effect=post_drain
            ):
                initial = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                rising = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(
                        pending_resources=ResourceQuantity(vcpu=1)
                    ),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                acknowledged = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(
                        pending_resources=ResourceQuantity(vcpu=1)
                    ),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
            intent = state.get_drain_intent("prod-a", "owned")

        self.assertEqual(initial["drainingJobIds"], ["owned"])
        self.assertEqual(rising["drainingJobIds"], [])
        self.assertEqual(rising["cancelingDrainJobIds"], ["owned"])
        self.assertEqual(rising["drainReadyStopJobIds"], [])
        self.assertEqual(rising["definitelyTerminatedJobIds"], [])
        self.assertEqual(acknowledged["cancelingDrainJobIds"], [])
        self.assertEqual(acknowledged["canceledDrainJobIds"], ["owned"])
        self.assertEqual(terminate_calls, [])
        self.assertEqual(
            [draining for _token, draining in drain_actions],
            [True, False, False],
        )
        self.assertIsNone(intent)

    def test_ambiguous_stop_retries_same_journal_and_preserves_heartbeat(self) -> None:
        terminate_calls: list[tuple[str, ...]] = []

        class AmbiguousStopClient:
            def __init__(self, _session_store) -> None:
                pass

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                terminate_calls.append(tuple(job_ids))
                raise UCloudError("connection dropped during terminate")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "owned",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-owned",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-2-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                        "ucloud-sandboxes/agent-version": package_version(),
                                    },
                                },
                                "status": {"state": "RUNNING"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "heartbeats.json"
            HeartbeatStore(heartbeat_file).save(
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        updated_at=utc_now(),
                        active_sandboxes=0,
                        idle_since=utc_now() - timedelta(minutes=10),
                        node_url="http://node-owned:8090",
                        agent_version=package_version(),
                        capabilities=("disk-quota",),
                    )
                }
            )
            config = AutoscalerConfig(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                state_dir=raw_dir,
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                heartbeats=heartbeat_file,
                include_job=[],
                all_vm_jobs=False,
                execute=False,
                execute_stops=True,
                execute_init=False,
                allow_unlabeled_stops=False,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
            original_client = cli.UCloudClient
            cli.UCloudClient = AmbiguousStopClient
            try:
                with patch.object(cli, "_post_node_drain", return_value={}):
                    first = cli.run_reconcile_cycle(
                        config,
                        args,
                        demand=SandboxDemand(),
                        provider_state=state,
                        provider_mutations_allowed=True,
                    )
                    intent = state.list_drain_intents(state="active")[0]
                    HeartbeatStore(heartbeat_file).save(
                        {
                            "owned": NodeHeartbeat(
                                node_id="node-owned",
                                job_id="owned",
                                updated_at=utc_now(),
                                active_sandboxes=0,
                                idle_since=utc_now() - timedelta(minutes=10),
                                node_url="http://node-owned:8090",
                                agent_version=package_version(),
                                capabilities=("disk-quota",),
                                draining=True,
                                admission_open=False,
                                drain_token=intent.token,
                                inventory_complete=True,
                                activity_epoch=4,
                                drain_activity_epoch=4,
                            )
                        }
                    )
                    second = cli.run_reconcile_cycle(
                        config,
                        args,
                        demand=SandboxDemand(),
                        provider_state=state,
                        provider_mutations_allowed=True,
                    )
                    HeartbeatStore(heartbeat_file).save(
                        {
                            "owned": NodeHeartbeat(
                                node_id="node-owned",
                                job_id="owned",
                                updated_at=utc_now(),
                                active_sandboxes=0,
                                node_url="http://node-owned:8090",
                                agent_version=package_version(),
                                capabilities=("disk-quota",),
                                draining=True,
                                admission_open=False,
                                drain_token=intent.token,
                                inventory_complete=True,
                                activity_epoch=5,
                                drain_activity_epoch=5,
                                reserved_resources=ResourceQuantity(vcpu=1),
                            )
                        }
                    )
                    third = cli.run_reconcile_cycle(
                        config,
                        args,
                        demand=SandboxDemand(),
                        provider_state=state,
                        provider_mutations_allowed=True,
                    )
                    HeartbeatStore(heartbeat_file).save(
                        {
                            "owned": NodeHeartbeat(
                                node_id="node-owned",
                                job_id="owned",
                                updated_at=utc_now(),
                                active_sandboxes=0,
                                node_url="http://node-owned:8090",
                                agent_version=package_version(),
                                capabilities=("disk-quota",),
                                draining=True,
                                admission_open=False,
                                drain_token=intent.token,
                                inventory_complete=True,
                                activity_epoch=6,
                                drain_activity_epoch=6,
                            )
                        }
                    )
                    fourth = cli.run_reconcile_cycle(
                        config,
                        args,
                        demand=SandboxDemand(),
                        provider_state=state,
                        provider_mutations_allowed=True,
                    )
            finally:
                cli.UCloudClient = original_client
            remaining = HeartbeatStore(heartbeat_file).load()

        self.assertEqual(terminate_calls, [("owned",), ("owned",)])
        self.assertEqual(first["providerOperationResults"], [])
        self.assertEqual(first["definitelyTerminatedJobIds"], [])
        self.assertEqual(second["providerOperationResults"][-1]["state"], "uncertain")
        self.assertEqual(second["definitelyTerminatedJobIds"], [])
        self.assertEqual(third["stopRecoveryResults"][0]["state"], "retry")
        self.assertEqual(third["definitelyTerminatedJobIds"], [])
        self.assertFalse(third["drainResults"][0]["heartbeatReady"])
        self.assertEqual(fourth["providerOperationResults"][-1]["state"], "uncertain")
        self.assertEqual(fourth["definitelyTerminatedJobIds"], [])
        self.assertIn("owned", remaining)

    def test_reconcile_prunes_heartbeats_for_final_jobs(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "finished-node",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-finished",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-16-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                        "ucloud-sandboxes/agent-version": package_version(),
                                    },
                                    "parameters": {"diskSize": {"value": 250}},
                                },
                                "status": {
                                    "state": "SUCCESS",
                                    "jobParametersJson": {
                                        "request": {
                                            "resolvedProduct": {
                                                "cpu": 16,
                                                "memoryInGigs": 32,
                                            },
                                        },
                                    },
                                },
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "heartbeats.json"
            HeartbeatStore(heartbeat_file).save(
                {
                    "finished-node": NodeHeartbeat(
                        node_id="node-finished",
                        job_id="finished-node",
                        updated_at=utc_now(),
                        active_sandboxes=0,
                        node_url="http://node-finished:8090",
                        agent_version=package_version(),
                        deployment_id="prod-a",
                        capabilities=("sandbox", "image-cache"),
                    )
                }
            )
            config = AutoscalerConfig(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                state_dir=raw_dir,
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                heartbeats=heartbeat_file,
                include_job=[],
                all_vm_jobs=False,
                execute=False,
                execute_stops=False,
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
            remaining_heartbeats = HeartbeatStore(heartbeat_file).load()

        self.assertEqual(result["prunedFinalHeartbeats"], ["finished-node"])
        self.assertIn("finished-node", remaining_heartbeats)

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
