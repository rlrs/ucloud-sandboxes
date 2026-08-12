import argparse
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import timedelta
from functools import wraps
import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
from types import SimpleNamespace
import unittest
from urllib.error import HTTPError
from unittest.mock import MagicMock, patch

from ucloud_sandboxes import cli
from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.autoscaler_state import (
    AutoscalerStateStore,
    ProviderOperationOutcome,
)
from ucloud_sandboxes.cli import (
    find_ucloud_ssh_key,
    read_public_ssh_key_file,
    should_include_job,
    submitted_job_ids,
    vm_submission_options_from_args,
)
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.images import ImageRecord, ImageStore
from ucloud_sandboxes.managed_registry import RegistryTag, RegistryUsageStore
from ucloud_sandboxes.metrics import MetricsStore
from ucloud_sandboxes.models import (
    InstancePhase,
    NodeHeartbeat,
    ResourceQuantity,
    SandboxDemand,
    SandboxInventoryEntry,
    SandboxNode,
    SandboxPlacementRequest,
    ScaleDecision,
    ScalePolicy,
    ProviderInstance,
    utc_now,
)
from ucloud_sandboxes.providers.base import ProviderMutationResult
from ucloud_sandboxes.routing import (
    RoutingStore,
    SandboxRoute,
    SandboxRouteAllocation,
)
from ucloud_sandboxes.providers.ucloud.api import UCloudError
from ucloud_sandboxes.providers.ucloud.config import UCloudSettings
from ucloud_sandboxes.providers.ucloud import UCloudCreateProfile, UCloudProvider
from ucloud_sandboxes.vm_init import VmInitPackageStageResult


def ucloud_config(**values) -> DeploymentConfig:
    settings = UCloudSettings.default()
    provider_values = {
        "project_id": values.pop("project_id", settings.project_id),
        "session_file": values.pop("ucloud_session_file", settings.session_file),
        "template_job_id": values.pop("template_job_id", settings.template_job_id),
        "private_network_id": values.pop(
            "private_network_id", settings.private_network_id
        ),
        "gateway_public_link_id": values.pop(
            "gateway_public_link_id", settings.gateway_public_link_id
        ),
        "gateway_public_link_port": values.pop(
            "gateway_public_link_port", settings.gateway_public_link_port
        ),
    }
    session_file = provider_values.pop("session_file")
    provider = (
        replace(settings, **provider_values)
        .to_provider()
        .with_setting("session_file", session_file)
    )
    data_root = values.pop("data_root", "/tmp/ucloud-state")
    state_path = Path(data_root)
    if state_path.is_dir():
        write_test_tokens(state_path)
    return replace(
        DeploymentConfig.default(scope_id=provider.scope_id or "project-1"),
        provider=provider,
        data_root=data_root,
        **values,
    )


def write_ucloud_config(root: Path, **values) -> Path:
    path = root / "deployment.json"
    values.setdefault("project_id", "project-1")
    values.setdefault("private_network_id", "net-1")
    path.write_text(
        json.dumps(ucloud_config(data_root=str(root), **values).to_dict()),
        encoding="utf-8",
    )
    return path


def write_test_tokens(root: Path) -> None:
    for name in (
        "gateway-token",
        "heartbeat-token",
        "node-control-token",
        "relay-sandbox-token",
        "relay-worker-token",
    ):
        (root / name).write_text("test-token\n", encoding="utf-8")


def sandbox_route(
    sandbox_id: str,
    *,
    node_id: str = "node-1",
    job_id: str = "job-1",
    node_url: str = "http://node-1:8090",
    resources: ResourceQuantity = ResourceQuantity(),
    spec: dict | None = None,
    state: str = "running",
    generation: int = 1,
    create_operation_id: str = "create-test",
    spec_hash: str = "a" * 64,
    **values,
) -> SandboxRoute:
    return SandboxRoute(
        sandbox_id=sandbox_id,
        node_id=node_id,
        job_id=job_id,
        node_url=node_url,
        resources=resources,
        spec=spec or {"id": sandbox_id},
        state=state,
        generation=generation,
        create_operation_id=create_operation_id,
        spec_hash=spec_hash,
        **values,
    )


def save_heartbeats(path: Path, heartbeats: dict[str, NodeHeartbeat]) -> None:
    store = ControlStateStore(path)
    for heartbeat in heartbeats.values():
        store.upsert_heartbeat(heartbeat)


def allow_fixture_mutations(test):
    """Run provider-journal unit cases against deterministic fixtures."""

    @wraps(test)
    def wrapped(*args, **kwargs):
        with patch.object(cli, "reject_mutating_jobs_fixture", return_value=None):
            return test(*args, **kwargs)

    return wrapped


def write_deploy_runtime_artifacts(root: Path) -> tuple[Path, Path, Path]:
    runsc = root / "runsc"
    runsc.write_bytes(b"patched-runsc")
    runsc.chmod(0o755)
    managed_init = root / "ucloud-sandbox-init"
    managed_init.write_bytes(b"managed-init")
    managed_init.chmod(0o755)
    backend_bytes = b"storage-native-backend"
    backend_digest = hashlib.sha256(backend_bytes).hexdigest()
    backend = root / f"uvm-ublk-daemon-{backend_digest}"
    backend.write_bytes(backend_bytes)
    backend.chmod(0o755)
    (root / f"{backend.name}.LICENSE").write_text("MIT\n", encoding="utf-8")
    manifest = root / f"{backend.name}.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "agentenv_commit": "db1492b7915a408b37f863c9e3a34b2ccb2fb1b0",
                "artifact": backend.name,
                "artifact_sha256": backend_digest,
                "cargo_package": "uvm-ublk-daemon",
                "host_architecture": "x86_64",
                "license": "MIT",
                "patches": [
                    {
                        "name": "agentenv-streaming-dense-export.patch",
                        "sha256": "a" * 64,
                    },
                    {
                        "name": "agentenv-pooled-delete.patch",
                        "sha256": "b" * 64,
                    },
                    {
                        "name": "agentenv-owner-identity.patch",
                        "sha256": "c" * 64,
                    },
                ],
                "schema": 3,
            }
        ),
        encoding="utf-8",
    )
    return runsc, managed_init, manifest


class CliTests(unittest.TestCase):
    def test_control_posts_share_auth_redirect_and_response_bounds(self) -> None:
        def opener_for(body: bytes) -> tuple[MagicMock, MagicMock]:
            response = MagicMock(headers={})
            response.__enter__.return_value = response
            response.read.return_value = body
            opener = MagicMock()
            opener.open.return_value = response
            return opener, response

        opener, response = opener_for(b'{"drain":{"ready":true}}')
        with patch.object(cli, "build_opener", return_value=opener) as build_opener:
            payload = cli._post_node_drain(
                "https://node.example/",
                "drain-1",
                bearer_token="secret",
                timeout_seconds=7.0,
            )
        request = opener.open.call_args.args[0]
        self.assertTrue(payload["drain"]["ready"])
        self.assertEqual(request.full_url, "https://node.example/v1/drain")
        self.assertEqual(request.get_header("Authorization"), "Bearer secret")
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 7.0)
        response.read.assert_called_once_with(cli._MAX_CONTROL_RESPONSE_BYTES + 1)
        self.assertIsInstance(
            build_opener.call_args.args[0], cli._RejectControlRedirects
        )

        with self.assertRaisesRegex(ValueError, "invalid node URL"):
            cli._post_node_drain("file:///tmp/node", "drain-1")
        with self.assertRaisesRegex(ValueError, "bearer token cannot be empty"):
            cli._post_gateway_sandbox_migration(
                "https://gateway.example", "sandbox", bearer_token=" "
            )

        invalid_opener, _response = opener_for(b"[]")
        with (
            patch.object(cli, "build_opener", return_value=invalid_opener),
            self.assertRaisesRegex(
                ValueError, "migration response must be a JSON object"
            ),
        ):
            cli._post_gateway_sandbox_migration(
                "https://gateway.example", "sandbox/one"
            )

        relay_request = SimpleNamespace(
            sandbox_id="sandbox/one",
            sandbox_generation=2,
            request_id="request-1",
            rollout_id="rollout-1",
            created_at=1.0,
        )
        oversized_opener, _response = opener_for(
            b"x" * (cli._MAX_CONTROL_RESPONSE_BYTES + 1)
        )
        with (
            patch.object(cli, "build_opener", return_value=oversized_opener),
            self.assertRaisesRegex(ValueError, "lifecycle response exceeds 1 MiB"),
        ):
            cli._post_gateway_sandbox_lifecycle(
                "https://gateway.example",
                "secret",
                relay_request,
                action="park",
            )

        conflict = HTTPError(
            "https://gateway.example",
            409,
            "lifecycle transition is in progress",
            {},
            io.BytesIO(b'{"error":"lifecycle transition is in progress"}'),
        )
        with (
            patch.object(
                cli,
                "_post_bounded_json",
                side_effect=[
                    conflict,
                    (
                        {"sandbox": {"state": "parked"}},
                        {"X-UCloud-Sandbox-Transport-Epoch": "epoch-1"},
                    ),
                ],
            ) as post,
            patch.object(cli.time, "sleep") as sleep,
        ):
            epoch = cli._post_gateway_sandbox_lifecycle(
                "https://gateway.example",
                "secret",
                relay_request,
                action="park",
            )
        self.assertEqual(epoch, "epoch-1")
        self.assertEqual(post.call_count, 2)
        sleep.assert_called_once_with(0.05)

    def test_executing_stop_waiting_for_drain_is_not_reported_as_dry_run(
        self,
    ) -> None:
        output = io.StringIO()
        decision = ScaleDecision(
            actions=(),
            ready_nodes=1,
            provisioning_nodes=0,
            total_nodes=1,
            reasons=("idle node exceeds min_nodes=0",),
        )
        with redirect_stdout(output):
            cli.print_reconcile(
                [],
                decision,
                Path("/tmp/control-state.sqlite"),
                [],
                (),
                {
                    "provider": {"kind": "ucloud", "scopeId": "project-1"},
                    "requestedStopJobIds": ["job-1"],
                    "blockedStopJobIds": [],
                    "execute": True,
                    "rawBootstrapIntents": [],
                },
            )

        self.assertIn("waiting for drain proof", output.getvalue())
        self.assertNotIn("Stop dry-run only", output.getvalue())

    def test_autoscaler_execute_rejects_jobs_fixture_before_provider_calls(
        self,
    ) -> None:
        class ForbiddenClient:
            def __init__(self, *_args, **_kwargs) -> None:
                raise AssertionError("provider client must not be constructed")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            config_file = write_ucloud_config(root, deployment_id="prod-a")
            stderr = io.StringIO()
            with patch.object(cli, "UCloudClient", ForbiddenClient):
                with redirect_stderr(stderr):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--execute",
                            "--once",
                        ]
                    )
            self.assertEqual(result, 1)
            self.assertIn("--jobs-file is dry-run only", stderr.getvalue())

    def test_removed_autoscaler_commands_have_no_aliases(self) -> None:
        parser = cli.build_parser()
        for command in ("plan", "reconcile", "autoscaler-loop"):
            with (
                self.subTest(command=command),
                self.assertRaises(SystemExit),
                redirect_stderr(io.StringIO()),
            ):
                parser.parse_args([command])

    def test_removed_configuration_flags_have_no_aliases(self) -> None:
        parser = cli.build_parser()
        for argv in (
            ("agent-heartbeat", "--cpu-overcommit", "2"),
            ("init-vm", "job-1", "--memory-overcommit", "2"),
            ("autoscaler", "--init-disk-overcommit", "1"),
            ("deploy-all-in-one", "--cpu-overcommit", "2"),
            ("agent-heartbeat", "--heartbeat-file", "old.json"),
            ("serve-control-plane", "--heartbeat-file", "old.json"),
            ("heartbeats", "--heartbeat-file", "old.json"),
            ("autoscaler", "--heartbeats", "old.json"),
            ("autoscaler", "--init-state-file", "old.json"),
        ):
            with (
                self.subTest(argv=argv),
                self.assertRaises(SystemExit),
                redirect_stderr(io.StringIO()),
            ):
                parser.parse_args(argv)

    def test_autoscaler_state_is_derived_from_deployment_root(self) -> None:
        config = ucloud_config(
            project_id="project-1",
            deployment_id="prod-a",
            data_root="/var/lib/ucloud-sandboxes",
        )

        self.assertEqual(
            config.autoscaler_state_file(),
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
                return RegistryTag(repository, tag, "sha256:" + "1" * 64)

            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted.append((repository, digest))

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            usage_file = root / "registry-usage.sqlite"
            RegistryUsageStore(usage_file).acquire_lease(
                "repo/a",
                "v1",
                "sandbox:1:generation:2",
                ttl_seconds=60,
                digest="sha256:" + "1" * 64,
            )
            output = io.StringIO()
            FakeRegistryClient.deleted = []
            config_file = write_ucloud_config(
                root,
                registry_keep_per_repository=0,
            )
            with patch.object(cli, "RegistryClient", FakeRegistryClient):
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "registry-prune",
                            "--config",
                            str(config_file),
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
            ProviderOperationOutcome(
                operation_id=f"operation-{index}",
                kind="create",
                role="sandbox",
                state="accepted",
                job_ids=(f"job-{index}",),
                source="planned",
            )
            for index in range(4)
        ]

        self.assertFalse(
            cli._sandbox_capacity_operation_succeeded(
                results,
                ResourceQuantity(vcpu=128, memory_mb=262144, disk_mb=524288),
                ResourceQuantity(vcpu=16, memory_mb=32768, disk_mb=204800),
            )
        )

    def test_no_create_operation_does_not_consume_pending_demand(self) -> None:
        self.assertFalse(
            cli._sandbox_capacity_operation_succeeded(
                [],
                ResourceQuantity(),
                ResourceQuantity(vcpu=32, memory_mb=39321, disk_mb=204800),
            )
        )

    def test_provider_http_rejection_and_ambiguity_are_journaled_differently(
        self,
    ) -> None:
        class RejectingProvider:
            def __init__(self, status: int) -> None:
                self.status = status

            def terminate(self, instance_ids) -> ProviderMutationResult:
                if self.status == 400:
                    return ProviderMutationResult(
                        status="rejected",
                        error="definite rejection",
                    )
                return ProviderMutationResult(
                    status="uncertain",
                    error="ambiguous provider outcome",
                )

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
                RejectingProvider(400),
                source="planned",
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
                RejectingProvider(503),
                source="planned",
                allowed_kinds={"stop"},
                allowed_stop_operation_ids={ambiguous.operation_id},
            )
            definite_state = state.get_operation(definite.operation_id).state
            ambiguous_state = state.get_operation(ambiguous.operation_id).state

        self.assertEqual(definite_result[0].state, "failed")
        self.assertEqual(definite_state, "failed")
        self.assertEqual(ambiguous_result[0].state, "uncertain")
        self.assertEqual(ambiguous_state, "uncertain")

    @allow_fixture_mutations
    def test_post_run_suspension_is_stopped_and_replaced_as_node_loss(
        self,
    ) -> None:
        submitted: list[dict] = []
        terminated: list[tuple[str, ...]] = []
        provider_calls: list[str] = []

        class ReplacementClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, _project_id: str, payload: dict) -> dict:
                provider_calls.append("create")
                submitted.append(payload)
                return {"responses": [{"id": "replacement-node"}]}

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                provider_calls.append("stop")
                terminated.append(tuple(job_ids))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "suspended-node",
                                "owner": {"project": "project-1"},
                                "createdAt": 1_700_000_000_000,
                                "specification": {
                                    "name": "ucloud-sandbox-node-suspended",
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
                                    "resources": [
                                        {"type": "private_network", "id": "net-1"}
                                    ],
                                },
                                "status": {
                                    "state": "RUNNING",
                                    "startedAt": 1_700_000_100_000,
                                },
                                "updates": [
                                    {"state": "RUNNING"},
                                    {"state": "SUSPENDED"},
                                    {"state": "RUNNING"},
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                private_network_id="net-1",
                data_root=raw_dir,
                policy=ScalePolicy(min_nodes=1, max_nodes=2),
            )
            args = cli.build_parser().parse_args(
                [
                    "autoscaler",
                    "--config",
                    str(root / "unused-deployment.json"),
                    "--jobs-file",
                    str(jobs_file),
                    "--execute",
                    "--seed-prefix",
                    "test",
                    "--once",
                ]
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
            lost_route = sandbox_route(
                sandbox_id="lost-sandbox",
                node_id="lost-node",
                job_id="suspended-node",
                node_url="http://lost-node:8090",
                resources=ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=8192),
                state="running",
                generation=1,
                create_operation_id="create-lost",
                spec_hash="a" * 64,
            )

            with patch.object(cli, "UCloudClient", ReplacementClient):
                result = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                    route_reservations={"suspended-node": (lost_route,)},
                    sandbox_routes=(lost_route,),
                )

        self.assertEqual(len(submitted), 1)
        self.assertEqual(terminated, [("suspended-node",)])
        self.assertEqual(provider_calls, ["stop", "create"])
        self.assertEqual(result["destructive_power_cycle_job_ids"], ["suspended-node"])
        self.assertEqual(result["lost_sandbox_ids"], ["lost-sandbox"])
        self.assertEqual(result["destructive_stop_job_ids"], ["suspended-node"])
        self.assertEqual(result["rawDecision"].total_nodes, 0)
        self.assertEqual(result["rawDecision"].creates, 1)

    @allow_fixture_mutations
    def test_fresh_replacement_epoch_still_retrieves_power_cycle_history(
        self,
    ) -> None:
        retrieve_calls: list[tuple[str, str, bool]] = []

        def job_payload(*, include_updates: bool) -> dict:
            payload = {
                "id": "replaced-job",
                "owner": {"project": "project-1"},
                "createdAt": 1_700_000_000_000,
                "specification": {
                    "name": "ucloud-sandbox-node-replaced",
                    "application": {
                        "name": "vm-ubuntu",
                        "version": "24.04",
                    },
                    "product": {
                        "id": "cpu-amd-zen5-32-vcpu",
                        "category": "cpu-amd-zen5",
                    },
                    "labels": {
                        "ucloud-sandboxes/node": "true",
                        "ucloud-sandboxes/deployment": "prod-a",
                    },
                    "resources": [{"type": "private_network", "id": "net-1"}],
                },
                "status": {
                    "state": "RUNNING",
                    "startedAt": 1_700_000_100_000,
                },
            }
            if include_updates:
                payload["updates"] = [
                    {"state": "RUNNING"},
                    {"state": "SUSPENDED"},
                    {"state": "RUNNING"},
                ]
            return payload

        class HistoryClient:
            def __init__(self, _session_store) -> None:
                pass

            def browse_all_jobs(self, *_args, **_kwargs) -> list[dict]:
                return [job_payload(include_updates=False)]

            def retrieve_job(
                self,
                project_id: str,
                job_id: str,
                *,
                include_updates: bool = False,
            ) -> dict:
                retrieve_calls.append((project_id, job_id, include_updates))
                return job_payload(include_updates=include_updates)

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            heartbeat_path = root / "control-state.sqlite"
            ControlStateStore(heartbeat_path).upsert_heartbeat(
                build_heartbeat(
                    job_id="replaced-job",
                    node_id="new-node",
                    deployment_id="test-deployment",
                    node_epoch="new-boot",
                    active_sandboxes=0,
                    now=utc_now(),
                )
            )
            route = sandbox_route(
                sandbox_id="lost-sandbox",
                node_id="old-node",
                job_id="replaced-job",
                node_url="http://old-node:8090",
                resources=ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=8192),
                state="running",
                generation=1,
                create_operation_id="create-lost",
                spec_hash="a" * 64,
                node_epoch="old-boot",
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                private_network_id="net-1",
                data_root=raw_dir,
                policy=ScalePolicy(min_nodes=1, max_nodes=2),
            )
            args = cli.build_parser().parse_args(
                [
                    "autoscaler",
                    "--config",
                    str(root / "unused-deployment.json"),
                    "--once",
                ]
            )

            with patch.object(cli, "UCloudClient", HistoryClient):
                result = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    route_reservations={"replaced-job": (route,)},
                    sandbox_routes=(route,),
                )

        self.assertEqual(
            retrieve_calls,
            [("project-1", "replaced-job", True)],
        )
        self.assertEqual(
            result["destructive_power_cycle_job_ids"],
            ["replaced-job"],
        )
        self.assertEqual(result["lost_sandbox_ids"], ["lost-sandbox"])

    @allow_fixture_mutations
    def test_initial_suspension_is_booting(self) -> None:
        class ProvisioningClient:
            def __init__(self, _session_store) -> None:
                pass

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "booting-node",
                                "owner": {"project": "project-1"},
                                "createdAt": 1_700_000_000_000,
                                "specification": {
                                    "name": "ucloud-sandbox-node-booting",
                                    "application": {"name": "vm-ubuntu"},
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                    },
                                },
                                "status": {"state": "SUSPENDED"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
                policy=ScalePolicy(min_nodes=1, max_nodes=2),
            )
            args = cli.build_parser().parse_args(
                [
                    "autoscaler",
                    "--config",
                    str(root / "unused-deployment.json"),
                    "--jobs-file",
                    str(jobs_file),
                    "--seed-prefix",
                    "test",
                    "--once",
                ]
            )

            with patch.object(cli, "UCloudClient", ProvisioningClient):
                result = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=AutoscalerStateStore(
                        root / "autoscaler-state.sqlite"
                    ),
                    provider_mutations_allowed=True,
                )

        self.assertEqual(result["unexpectedly_suspended_job_ids"], [])
        self.assertEqual(result["rawDecision"].total_nodes, 1)
        self.assertEqual(result["rawDecision"].provisioning_nodes, 1)
        self.assertEqual(result["providerOperationResults"], [])

    @allow_fixture_mutations
    def test_autoscaler_loop_fences_and_removes_power_cycled_node_routes(self) -> None:
        submitted: list[dict] = []
        terminated: list[tuple[str, ...]] = []

        class ReplacementClient:
            def __init__(self, _session_store) -> None:
                pass

            def submit_jobs(self, _project_id: str, payload: dict) -> dict:
                submitted.append(payload)
                return {"responses": [{"id": "replacement-node"}]}

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                terminated.append(tuple(job_ids))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "lost-job",
                                "createdAt": 1_700_000_000_000,
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-lost",
                                    "application": {
                                        "name": "vm-ubuntu",
                                        "version": "24.04",
                                    },
                                    "product": {
                                        "id": "cpu-amd-zen5-32-vcpu",
                                        "category": "cpu-amd-zen5",
                                    },
                                    "labels": {
                                        "ucloud-sandboxes/node": "true",
                                        "ucloud-sandboxes/deployment": "prod-a",
                                    },
                                    "resources": [
                                        {"type": "private_network", "id": "net-1"}
                                    ],
                                    "parameters": {"diskSize": {"value": 2000}},
                                },
                                "status": {
                                    "state": "SUSPENDED",
                                    "startedAt": 1_700_000_100_000,
                                },
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            route_file = root / "routes.sqlite"
            RoutingStore(route_file).upsert_sandbox(
                sandbox_route(
                    sandbox_id="lost-sandbox",
                    node_id="lost-node",
                    job_id="lost-job",
                    node_url="http://lost-node:8090",
                    resources=ResourceQuantity(
                        vcpu=2,
                        memory_mb=4096,
                        disk_mb=8192,
                    ),
                    spec={"id": "lost-sandbox"},
                    state="running",
                    generation=1,
                    create_operation_id="create-lost",
                    spec_hash="a" * 64,
                )
            )
            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with patch.object(cli, "UCloudClient", ReplacementClient):
                with redirect_stdout(output):
                    exit_code = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--execute",
                            "--once",
                            "--output",
                            "json",
                        ]
                    )
            payload = json.loads(output.getvalue())
            routes = RoutingStore(route_file).sandbox_routes_readonly()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(submitted), 1)
        self.assertEqual(terminated, [("lost-job",)])
        self.assertEqual(routes, [])
        self.assertEqual(
            [item["sandbox_id"] for item in payload["removedRoutes"]],
            ["lost-sandbox"],
        )
        self.assertEqual(payload["persistedNodeLossDemand"], [])

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
        config = DeploymentConfig.default(scope_id="project-1")
        provider = UCloudSettings.from_provider(config.provider)
        config = ucloud_config(
            project_id=provider.project_id,
            deployment_id="prod-a",
            private_network_id="net-1",
            ucloud_session_file=provider.session_file,
            data_root=config.data_root,
            policy=config.policy,
        )
        matching = ProviderInstance(
            id="job-1",
            name="ucloud-sandbox-node-1",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id=None,
            product_category="cpu-amd-zen5",
            state="RUNNING",
            phase=InstancePhase.RUNNING,
            private_network_ids=("net-1",),
            labels={
                "ucloud-sandboxes/deployment": "prod-a",
                "ucloud-sandboxes/node": "true",
            },
        )
        wrong_network = ProviderInstance(
            id="job-2",
            name="ucloud-sandbox-node-2",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="RUNNING",
            phase=InstancePhase.RUNNING,
            private_network_ids=("net-2",),
            labels={
                "ucloud-sandboxes/deployment": "prod-a",
                "ucloud-sandboxes/node": "true",
            },
        )
        compute_provider = UCloudProvider(
            "project-1",
            sandbox_profile=UCloudCreateProfile(private_network_id="net-1"),
            builder_profile=UCloudCreateProfile(private_network_id="net-1"),
        )

        self.assertTrue(should_include_job(matching, config, compute_provider, set()))
        self.assertFalse(
            should_include_job(wrong_network, config, compute_provider, set())
        )
        self.assertFalse(
            should_include_job(
                replace(matching, id="unlabeled", labels={}),
                config,
                compute_provider,
                set(),
            )
        )
        self.assertTrue(
            should_include_job(
                wrong_network,
                config,
                compute_provider,
                {"job-2"},
            )
        )

    def test_pool_role_is_never_inferred_from_job_name(self) -> None:
        def node(name: str, labels: dict[str, str]) -> SandboxNode:
            return SandboxNode(
                job=ProviderInstance(
                    id=name,
                    name=name,
                    application_name="vm-ubuntu",
                    application_version="24.04",
                    product_id=None,
                    product_category="cpu-amd-zen5",
                    state="RUNNING",
                    phase=InstancePhase.RUNNING,
                    labels=labels,
                ),
                heartbeat=None,
                active_sandboxes=0,
                heartbeat_fresh=False,
            )

        named_sandbox = node("ucloud-sandbox-node-1", {})
        named_builder = node("ucloud-sandbox-builder-1", {})
        labeled_sandbox = node(
            "arbitrary-sandbox",
            {"ucloud-sandboxes/node": "true"},
        )
        labeled_builder = node(
            "arbitrary-builder",
            {"ucloud-sandboxes/builder": "true"},
        )

        self.assertEqual(cli.sandbox_pool_nodes([named_sandbox, named_builder]), [])
        self.assertEqual(cli.builder_pool_nodes([named_sandbox, named_builder]), [])
        self.assertEqual(cli.sandbox_pool_nodes([labeled_sandbox]), [labeled_sandbox])
        self.assertEqual(cli.builder_pool_nodes([labeled_builder]), [labeled_builder])

    def test_metrics_path_is_derived_from_deployment_root(self) -> None:
        config = ucloud_config(
            project_id="project-1",
            data_root="/tmp/default-state",
            ucloud_session_file="/tmp/session.json",
        )

        self.assertEqual(
            config.metrics_path(),
            Path("/tmp/default-state/metrics.sqlite"),
        )

    def test_vm_submission_options_use_private_network_config(self) -> None:
        config = ucloud_config(
            project_id="project-1",
            private_network_id="12345327",
            ucloud_session_file="/tmp/session.json",
            data_root="/tmp/state",
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
        config = ucloud_config(
            project_id="project-1",
            private_network_id="12345327",
            ucloud_session_file="/tmp/session.json",
            data_root="/tmp/state",
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
        config = ucloud_config(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            data_root="/tmp/state",
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
            product_id=None,
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
        self.assertEqual(options.product.id, "cpu-amd-zen5-2-vcpu")
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
        config = ucloud_config(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            data_root="/tmp/state",
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
            product_id=None,
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
        config = ucloud_config(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            data_root="/tmp/state",
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
            product_id=None,
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
        self.assertEqual(options.product.id, "cpu-amd-zen5-32-vcpu")
        self.assertEqual(
            options.job_item()["resources"],
            [{"type": "private_network", "id": "12345327"}],
        )

    def test_builder_role_uses_builder_identity_without_node_label(self) -> None:
        config = ucloud_config(
            project_id="project-1",
            private_network_id="12345327",
            gateway_public_link_id="12345368",
            gateway_public_link_port=8090,
            ucloud_session_file="/tmp/session.json",
            data_root="/tmp/state",
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
            route_file = root / "routes.sqlite"
            RoutingStore(route_file).upsert_pending(
                "pending-one",
                ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
            )

            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler",
                        "--config",
                        str(config_file),
                        "--jobs-file",
                        str(jobs_file),
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
                                    "resources": [
                                        {"type": "private_network", "id": "net-1"}
                                    ],
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

            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler",
                        "--config",
                        str(config_file),
                        "--jobs-file",
                        str(jobs_file),
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

    @allow_fixture_mutations
    def test_autoscaler_execute_prunes_routes_for_final_jobs(self) -> None:
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
                                    "resources": [
                                        {"type": "private_network", "id": "net-1"}
                                    ],
                                    "parameters": {"diskSize": {"value": 250}},
                                },
                                "status": {"state": "SUCCESS"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "old-node": NodeHeartbeat(
                        node_id="node-old",
                        job_id="old-node",
                        deployment_id="prod-a",
                        updated_at=utc_now(),
                        active_sandboxes=1,
                        node_url="http://node-old:8090",
                    )
                },
            )
            route_file = root / "routes.sqlite"
            RoutingStore(route_file).upsert_sandbox(
                sandbox_route(
                    sandbox_id="stale-sandbox",
                    node_id="node-old",
                    job_id="old-node",
                    node_url="http://node-old:8090",
                    resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                    spec={"id": "stale-sandbox"},
                    generation=1,
                    create_operation_id="create-" + "1" * 32,
                    spec_hash="a" * 64,
                )
            )

            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler",
                        "--config",
                        str(config_file),
                        "--jobs-file",
                        str(jobs_file),
                        "--execute",
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
            route_store = RoutingStore(route_file)
            with patch(
                "ucloud_sandboxes.routing.utc_now",
                return_value=now - timedelta(seconds=600),
            ):
                route_store.upsert_sandbox(
                    sandbox_route(
                        sandbox_id="old-orphan",
                        node_id="old-node",
                        job_id="old-job",
                        node_url="http://old-node:8090",
                        spec={"id": "old-orphan"},
                        generation=1,
                        create_operation_id="create-" + "2" * 32,
                        spec_hash="b" * 64,
                        created_at=old,
                        updated_at=old,
                    )
                )
            with patch(
                "ucloud_sandboxes.routing.utc_now",
                return_value=now - timedelta(seconds=30),
            ):
                route_store.upsert_sandbox(
                    sandbox_route(
                        sandbox_id="recent-orphan",
                        node_id="recent-node",
                        job_id="recent-job",
                        node_url="http://recent-node:8090",
                        spec={"id": "recent-orphan"},
                        generation=1,
                        create_operation_id="create-" + "3" * 32,
                        spec_hash="c" * 64,
                        created_at=recent,
                        updated_at=recent,
                    )
                )

            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler",
                        "--config",
                        str(config_file),
                        "--jobs-file",
                        str(jobs_file),
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
            root = Path(raw_dir)
            wheel = root / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
            wheel.write_bytes(b"wheel")
            runsc, managed_init, storage_manifest = write_deploy_runtime_artifacts(root)
            config_file = write_ucloud_config(
                root,
                deployment_id="prod-a",
                gateway_private_host="sandbox-gateway-prod",
                registry_private_ip="10.0.0.5",
            )
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "deploy-all-in-one",
                        "job-1",
                        "--config",
                        str(config_file),
                        "--ssh-command",
                        "ssh ucloud@example.org -p 2222",
                        "--wheel",
                        str(wheel),
                        "--direct-runsc",
                        str(runsc),
                        "--managed-init",
                        str(managed_init),
                        "--storage-native-manifest",
                        str(storage_manifest),
                        "--output",
                        "json",
                    ]
                )

            payload = json.loads(output.getvalue())

        self.assertEqual(result, 0)
        self.assertFalse(payload["execute"])
        deployment = payload["plan"]["deployment"]
        self.assertEqual(deployment["deployment_id"], "prod-a")
        self.assertEqual(
            deployment["gateway_private_host"],
            "sandbox-gateway-prod",
        )
        self.assertEqual(deployment["registry_private_ip"], "10.0.0.5")
        self.assertEqual(deployment["data_root"], str(root))

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
                route_file = root / "routes.sqlite"
                RoutingStore(route_file).upsert_pending(
                    "pending-one",
                    ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
                )
                RoutingStore(route_file).upsert_pending(
                    "failed-image",
                    ResourceQuantity(vcpu=8.0, memory_mb=16384, disk_mb=32768),
                    failure_reason="image_pull_http_503",
                )

                config_file = write_ucloud_config(root, deployment_id="prod-a")
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--once",
                            "--execute",
                            "--output",
                            "json",
                        ]
                    )

                payload = json.loads(output.getvalue())
                remaining_demand = RoutingStore(route_file).pending_demand()
                remaining_pending = RoutingStore(route_file).pending_sandboxes()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(payload["createdJobIds"], ["created-node"])
        self.assertEqual(
            [item["sandbox_id"] for item in payload["consumedPendingDemand"]],
            ["pending-one"],
        )
        self.assertEqual(
            [item.sandbox_id for item in remaining_pending],
            ["failed-image"],
        )
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

                config_file = write_ucloud_config(root, deployment_id="prod-a")
                output = io.StringIO()
                stderr = io.StringIO()
                try:
                    with redirect_stdout(output), redirect_stderr(stderr):
                        result = cli.main(
                            [
                                "autoscaler",
                                "--config",
                                str(config_file),
                                "--jobs-file",
                                str(jobs_file),
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

    def test_autoscaler_dry_run_does_not_construct_provider_client(self) -> None:
        class FailingUCloudClient:
            def __init__(self, _session_store) -> None:
                raise AssertionError(
                    "autoscaler dry-run must not construct a provider client"
                )

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with patch.object(cli, "UCloudClient", FailingUCloudClient):
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--once",
                            "--output",
                            "json",
                        ]
                    )
            payload = json.loads(output.getvalue())

        self.assertEqual(result, 0)
        self.assertFalse(payload["execute"])

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
            config_file = write_ucloud_config(root, deployment_id="prod-a")
            command = [
                "autoscaler",
                "--config",
                str(config_file),
                "--jobs-file",
                str(jobs_file),
                "--seed-prefix",
                "one-shot",
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
                                    "createdAt": int(utc_now().timestamp() * 1000),
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
        recovery = second["providerOperationResults"][0]
        self.assertEqual(recovery["state"], "recovered")
        self.assertEqual(recovery["source"], "inventory-recovery")
        self.assertEqual(recovery["jobIds"], ["recovered-job"])
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0].state, "settled")
        self.assertTrue(
            first["autoscalerStateFile"].endswith("autoscaler-state.sqlite")
        )
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

        def post_drain(_node_url: str, token: str, **_kwargs) -> dict:
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
                                    "resources": [
                                        {"type": "private_network", "id": "net-1"}
                                    ],
                                },
                                "status": {"state": "RUNNING"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        deployment_id="prod-a",
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
                        resources_known=True,
                    ),
                },
            )
            config_file = write_ucloud_config(
                root,
                deployment_id="prod-a",
                policy=replace(ScalePolicy(), scale_down_idle_seconds=0),
            )
            command = [
                "autoscaler",
                "--config",
                str(config_file),
                "--jobs-file",
                str(jobs_file),
                "--execute",
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
                (intent,) = state.pending_drain_intents(deployment_id="prod-a")

                save_heartbeats(
                    heartbeat_file,
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
                            resources_known=True,
                            draining=True,
                            admission_open=False,
                            drain_token=intent.token,
                            inventory_complete=True,
                            activity_epoch=7,
                            drain_activity_epoch=7,
                        )
                    },
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
                route_file = root / "routes.sqlite"
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
                config_file = write_ucloud_config(root, deployment_id="prod-a")
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
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
                config_file = write_ucloud_config(root, deployment_id="prod-a")
                command = [
                    "autoscaler",
                    "--config",
                    str(config_file),
                    "--jobs-file",
                    str(jobs_file),
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
                                    "createdAt": int(utc_now().timestamp() * 1000),
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
        self.assertEqual(second["providerOperationResults"][0]["state"], "recovered")
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
                config_file = write_ucloud_config(root, deployment_id="prod-a")
                command = [
                    "autoscaler",
                    "--config",
                    str(config_file),
                    "--jobs-file",
                    str(jobs_file),
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
                                    "createdAt": int(utc_now().timestamp() * 1000),
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
    def test_autoscaler_keeps_prepared_capacity_until_sandboxes_claim_it(self) -> None:
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
                route_file = root / "routes.sqlite"
                RoutingStore(route_file).upsert_prepared_capacity(
                    "eval-soon",
                    ResourceQuantity(vcpu=1.0, memory_mb=2048, disk_mb=8192),
                    count=4,
                    ttl_seconds=600,
                )

                config_file = write_ucloud_config(root, deployment_id="prod-a")
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--once",
                            "--execute",
                            "--output",
                            "json",
                        ]
                    )

                payload = json.loads(output.getvalue())
                store = RoutingStore(route_file)
                demand_after_scale = store.pending_demand()
                for index in range(4):
                    store.allocate_sandbox_create_with_pending(
                        SandboxRouteAllocation(
                            sandbox_id=f"sandbox-{index}",
                            node_id="node-1",
                            job_id="created-node",
                            node_url="http://node-1:8090",
                            resources=ResourceQuantity(
                                vcpu=1,
                                memory_mb=2048,
                                disk_mb=8192,
                            ),
                            spec={"id": f"sandbox-{index}", "image": "busybox"},
                        ),
                        spec_hash=f"{index + 1:064x}",
                    )
                demand_after_claims = store.pending_demand()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(result, 0)
        self.assertEqual(submitted[0][0], "project-1")
        self.assertEqual(payload["createdJobIds"], ["created-node"])
        self.assertEqual(payload["decision"]["preparedResources"]["vcpu"], 4.0)
        self.assertEqual(demand_after_scale.prepared_resources.vcpu, 4)
        self.assertEqual(demand_after_claims.prepared_resources, ResourceQuantity())

    def test_autoscaler_loop_once_uses_route_file_pending_image_builds(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            jobs_file = root / "jobs.json"
            jobs_file.write_text('{"items": []}', encoding="utf-8")
            route_file = root / "routes.sqlite"
            RoutingStore(route_file).upsert_pending_image_build(
                "custom",
                "registry.example.org/custom:latest",
            )

            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler",
                        "--config",
                        str(config_file),
                        "--jobs-file",
                        str(jobs_file),
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
            {"vcpu": 1.0, "memory_mb": 512, "disk_mb": 1024},
        )
        self.assertEqual(payload["decision"]["actions"][0]["kind"], "create")
        self.assertEqual(payload["builderDecision"]["actions"][0]["kind"], "create")
        labels = {item["role"]: item["labels"] for item in payload["createIntents"]}
        self.assertEqual(labels["sandbox"]["ucloud-sandboxes/node"], "true")
        self.assertEqual(labels["builder"]["ucloud-sandboxes/builder"], "true")
        self.assertNotIn("ucloud-sandboxes/node", labels["builder"])

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

                config_file = write_ucloud_config(root, deployment_id="prod-a")
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
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
            ),
        )

        resources = cli.build_activity_sandbox_warm_resources(
            active_image_builds=1,
            pending_image_builds=0,
            prepared_builder_count=0,
            policy=policy,
        )
        demand = cli.demand_with_build_warm_resources(SandboxDemand(), resources)

        self.assertEqual(
            resources,
            ResourceQuantity(vcpu=1.0, memory_mb=512, disk_mb=1024),
        )
        self.assertEqual(
            demand.prepared_resources,
            ResourceQuantity(vcpu=1.0, memory_mb=512, disk_mb=1024),
        )

    def test_prepared_builder_alone_does_not_reserve_a_sandbox_node(self) -> None:
        resources = cli.build_activity_sandbox_warm_resources(
            active_image_builds=0,
            pending_image_builds=0,
            prepared_builder_count=1,
            policy=ScalePolicy(),
        )

        self.assertEqual(resources, ResourceQuantity())

    def test_build_warm_resources_supplement_instead_of_double_counting_demand(
        self,
    ) -> None:
        warm = ResourceQuantity(vcpu=32, memory_mb=39321, disk_mb=204800)
        demand = SandboxDemand(
            pending_resources=ResourceQuantity(vcpu=16, memory_mb=8192, disk_mb=16384),
            prepared_placement_requests=(
                SandboxPlacementRequest(
                    resources=ResourceQuantity(
                        vcpu=33,
                        memory_mb=16896,
                        disk_mb=33792,
                    )
                ),
            ),
        )

        combined = cli.demand_with_build_warm_resources(demand, warm)

        self.assertEqual(
            combined.desired_resources,
            ResourceQuantity(vcpu=49, memory_mb=39321, disk_mb=204800),
        )

    def test_route_reconciliation_reserves_only_hard_disk(self) -> None:
        routes = [
            sandbox_route(
                sandbox_id=f"sandbox-{index}",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=4096),
                state="running",
            )
            for index in range(64)
        ]
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            deployment_id="prod-a",
            updated_at=utc_now(),
            active_sandboxes=0,
            total_resources=ResourceQuantity(
                vcpu=32,
                memory_mb=98304,
                disk_mb=1_449_984,
            ),
            resources_known=True,
            capabilities=("disk-quota",),
        )

        reconciled = cli.apply_route_reservations_to_heartbeats(
            {"job-1": heartbeat},
            cli.sandbox_route_reservations(routes),
        )["job-1"]

        self.assertEqual(reconciled.active_sandboxes, 64)
        self.assertEqual(
            reconciled.used_resources,
            ResourceQuantity(disk_mb=64 * 4096),
        )
        self.assertEqual(reconciled.free_resources.vcpu, 32)
        self.assertEqual(reconciled.free_resources.memory_mb, 98304)

    def test_route_reservations_remain_visible_without_a_heartbeat(self) -> None:
        job = ProviderInstance(
            id="job-1",
            name="ucloud-sandbox-node-1",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu",
            product_category="cpu",
            state="RUNNING",
            phase=InstancePhase.RUNNING,
        )
        node = SandboxNode(
            job=job,
            heartbeat=None,
            active_sandboxes=0,
            heartbeat_fresh=False,
        )
        route = sandbox_route(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            state="running",
        )

        reconciled = cli.apply_route_reservations_to_nodes(
            [node],
            {"job-1": (route,)},
        )

        self.assertEqual(reconciled[0].active_sandboxes, 1)

    def test_parked_inventory_keeps_its_node_owned(self) -> None:
        job = ProviderInstance(
            id="job-1",
            name="ucloud-sandbox-node-1",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu",
            product_category="cpu",
            state="RUNNING",
            phase=InstancePhase.RUNNING,
        )
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            deployment_id="prod-a",
            updated_at=utc_now(),
            active_sandboxes=0,
            capabilities=("disk-quota",),
            inventory_complete=True,
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id="sandbox-1",
                    state="parked",
                    generation=1,
                    operation_id="00000000-0000-4000-8000-000000000001",
                    spec_hash="a" * 64,
                ),
            ),
            used_resources=ResourceQuantity(disk_mb=8192),
        )
        node = SandboxNode(
            job=job,
            heartbeat=heartbeat,
            active_sandboxes=0,
            heartbeat_fresh=True,
        )
        route = sandbox_route(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            state="parked",
        )

        reconciled = cli.apply_route_reservations_to_nodes(
            [node],
            {"job-1": (route,)},
        )

        self.assertEqual(reconciled[0].active_sandboxes, 1)
        self.assertFalse(reconciled[0].is_idle)

    @allow_fixture_mutations
    def test_direct_runtime_drain_keeps_owned_parked_inventory(self) -> None:
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
                            },
                            {
                                "id": "destination",
                                "owner": {"project": "project-1"},
                                "specification": {
                                    "name": "ucloud-sandbox-node-destination",
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
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            route = sandbox_route(
                sandbox_id="parked-one",
                node_id="node-owned",
                job_id="owned",
                node_url="http://node-owned:8090",
                resources=ResourceQuantity(
                    vcpu=1,
                    memory_mb=1024,
                    disk_mb=8192,
                ),
                state="parked",
            )
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        deployment_id="prod-a",
                        updated_at=utc_now(),
                        active_sandboxes=0,
                        idle_since=utc_now() - timedelta(minutes=10),
                        node_url=route.node_url,
                        agent_version=package_version(),
                        capabilities=("disk-quota",),
                        total_resources=ResourceQuantity(
                            vcpu=2,
                            memory_mb=6144,
                            disk_mb=51200,
                        ),
                        resources_known=True,
                        used_resources=ResourceQuantity(disk_mb=8192),
                        inventory_complete=True,
                        inventory=(
                            SandboxInventoryEntry(
                                sandbox_id=route.sandbox_id,
                                state="parked",
                                resources=route.resources,
                                generation=1,
                                operation_id=("00000000-0000-4000-8000-000000000001"),
                                spec_hash="a" * 64,
                            ),
                        ),
                    ),
                    "destination": NodeHeartbeat(
                        node_id="node-destination",
                        job_id="destination",
                        deployment_id="prod-a",
                        updated_at=utc_now(),
                        # Keep this ready node out of the scale-down candidate set.
                        active_sandboxes=1,
                        node_url="http://node-destination:8090",
                        agent_version=package_version(),
                        capabilities=("disk-quota",),
                        total_resources=ResourceQuantity(
                            vcpu=2,
                            memory_mb=6144,
                            disk_mb=51200,
                        ),
                        resources_known=True,
                        used_resources=ResourceQuantity(),
                        inventory_complete=True,
                    ),
                },
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
                policy=ScalePolicy(
                    max_stop_per_cycle=1,
                    scale_down_idle_seconds=0,
                ),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                control_state_file=heartbeat_file,
                include_job=[],
                execute=True,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
                init_heartbeat_url="",
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            class UnusedUCloudClient:
                def __init__(self, _session_store) -> None:
                    pass

            with patch.object(cli, "UCloudClient", UnusedUCloudClient):
                result = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                    route_reservations={"owned": (route,)},
                )

        self.assertEqual(result["requestedStopJobIds"], [])
        self.assertEqual(result["drainingJobIds"], [])
        self.assertEqual(result["drainReadyStopJobIds"], [])

    def test_last_direct_disk_owner_is_not_replaced_just_to_scale_down(self) -> None:
        route = sandbox_route(
            sandbox_id="parked-one",
            node_id="node-owned",
            job_id="owned",
            node_url="http://node-owned:8090",
            resources=ResourceQuantity(disk_mb=8192),
            state="parked",
        )
        heartbeat = NodeHeartbeat(
            node_id="node-owned",
            job_id="owned",
            deployment_id="prod-a",
            updated_at=utc_now(),
            active_sandboxes=0,
            capabilities=("disk-quota",),
            total_resources=ResourceQuantity(disk_mb=51200),
            resources_known=True,
            used_resources=route.resources,
            inventory_complete=True,
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id=route.sandbox_id,
                    state="parked",
                    resources=route.resources,
                    generation=1,
                    operation_id="00000000-0000-4000-8000-000000000001",
                    spec_hash="a" * 64,
                ),
            ),
        )
        owner = SandboxNode(
            job=ProviderInstance(
                id="owned",
                name="ucloud-sandbox-node-owned",
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu",
                product_category="cpu",
                state="RUNNING",
                phase=InstancePhase.RUNNING,
            ),
            heartbeat=heartbeat,
            active_sandboxes=0,
            heartbeat_fresh=True,
        )

        allowed, blocked = cli.partition_storage_native_detachable_stop_job_ids(
            [owner],
            ("owned",),
            {"owned": (route,)},
        )

        self.assertEqual(allowed, ())
        self.assertEqual(blocked, ("owned",))

    def test_published_owner_can_scale_down_without_a_destination_worker(self) -> None:
        route = sandbox_route(
            sandbox_id="parked-one",
            node_id="node-owned",
            job_id="owned",
            node_url="http://node-owned:8090",
            resources=ResourceQuantity(disk_mb=8192),
            state="parked",
            storage_schema="storage-native-v1",
            snapshot_manifest_digest="sha256:" + "b" * 64,
            snapshot_repository="snapshots",
            snapshot_tag="sandbox-1",
            storage_snapshot={"schema": "storage-native-v1"},
        )
        heartbeat = NodeHeartbeat(
            node_id="node-owned",
            job_id="owned",
            deployment_id="prod-a",
            updated_at=utc_now(),
            active_sandboxes=0,
            capabilities=(
                "disk-quota",
                "storage-native-v1",
                "sandbox-detach-published-v1",
            ),
            total_resources=ResourceQuantity(disk_mb=51200),
            resources_known=True,
            inventory_complete=True,
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id=route.sandbox_id,
                    state="parked",
                    resources=route.resources,
                    generation=route.generation,
                    operation_id=route.create_operation_id,
                    spec_hash=route.spec_hash,
                ),
            ),
        )
        owner = SandboxNode(
            job=ProviderInstance(
                id="owned",
                name="ucloud-sandbox-node-owned",
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu",
                product_category="cpu",
                state="RUNNING",
                phase=InstancePhase.RUNNING,
            ),
            heartbeat=heartbeat,
            active_sandboxes=0,
            heartbeat_fresh=True,
        )

        allowed, blocked = cli.partition_storage_native_detachable_stop_job_ids(
            [owner],
            ("owned",),
            {"owned": (route,)},
        )

        self.assertEqual(allowed, ("owned",))
        self.assertEqual(blocked, ())

    def test_unpublished_park_can_publish_during_destination_free_scale_down(
        self,
    ) -> None:
        route = sandbox_route(
            sandbox_id="parked-one",
            node_id="node-owned",
            job_id="owned",
            node_url="http://node-owned:8090",
            resources=ResourceQuantity(disk_mb=8192),
            state="parked",
        )
        heartbeat = NodeHeartbeat(
            node_id="node-owned",
            job_id="owned",
            deployment_id="prod-a",
            updated_at=utc_now(),
            active_sandboxes=0,
            capabilities=(
                "disk-quota",
                "storage-native-v1",
                "sandbox-detach-published-v1",
            ),
            total_resources=ResourceQuantity(disk_mb=51200),
            resources_known=True,
            inventory_complete=True,
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id=route.sandbox_id,
                    state="parked",
                    resources=route.resources,
                    generation=route.generation,
                    operation_id=route.create_operation_id,
                    spec_hash=route.spec_hash,
                ),
            ),
        )
        owner = SandboxNode(
            job=ProviderInstance(
                id="owned",
                name="ucloud-sandbox-node-owned",
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu",
                product_category="cpu",
                state="RUNNING",
                phase=InstancePhase.RUNNING,
            ),
            heartbeat=heartbeat,
            active_sandboxes=0,
            heartbeat_fresh=True,
        )

        allowed, blocked = cli.partition_storage_native_detachable_stop_job_ids(
            [owner],
            ("owned",),
            {"owned": (route,)},
        )

        self.assertEqual(allowed, ("owned",))
        self.assertEqual(blocked, ())

    def test_detached_routes_do_not_reserve_their_last_worker(self) -> None:
        route = sandbox_route(
            sandbox_id="parked-one",
            node_id="node-old",
            job_id="old",
            node_url="http://node-old:8090",
            state="parked",
            worker_state="detached",
            storage_schema="storage-native-v1",
            snapshot_manifest_digest="sha256:" + "b" * 64,
            snapshot_repository="snapshots",
            snapshot_tag="sandbox-1",
            storage_snapshot={"schema": "storage-native-v1"},
        )

        reservations = cli.sandbox_route_reservations((route,))

        self.assertEqual(reservations, {})

    def test_reconcile_detaches_park_before_provider_stop(self) -> None:
        terminate_calls: list[tuple[str, ...]] = []
        detach_calls: list[str] = []

        class SuccessfulStopClient:
            def __init__(self, _session_store) -> None:
                pass

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                terminate_calls.append(tuple(job_ids))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        route = sandbox_route(
            sandbox_id="parked-one",
            node_id="node-owned",
            job_id="owned",
            node_url="http://node-owned:8090",
            resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=8192),
            state="parked",
        )
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
            heartbeat_file = root / "control-state.sqlite"

            def heartbeat(*, token: str = "", inventory=True) -> NodeHeartbeat:
                entries = (
                    (
                        SandboxInventoryEntry(
                            sandbox_id=route.sandbox_id,
                            state="parked",
                            resources=route.resources,
                            generation=route.generation,
                            operation_id=route.create_operation_id,
                            spec_hash=route.spec_hash,
                        ),
                    )
                    if inventory
                    else ()
                )
                return NodeHeartbeat(
                    node_id="node-owned",
                    job_id="owned",
                    deployment_id="prod-a",
                    updated_at=utc_now(),
                    active_sandboxes=0,
                    idle_since=utc_now() - timedelta(minutes=10),
                    node_url="http://node-owned:8090",
                    agent_version=package_version(),
                    capabilities=(
                        "disk-quota",
                        "storage-native-v1",
                        "sandbox-detach-published-v1",
                    ),
                    total_resources=ResourceQuantity(
                        vcpu=2,
                        memory_mb=6144,
                        disk_mb=51200,
                    ),
                    resources_known=True,
                    draining=bool(token),
                    admission_open=not bool(token),
                    drain_token=token,
                    activity_epoch=7,
                    drain_activity_epoch=7 if token else 0,
                    inventory_complete=True,
                    inventory=entries,
                )

            save_heartbeats(heartbeat_file, {"owned": heartbeat()})
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                control_state_file=heartbeat_file,
                include_job=[],
                execute=True,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            def post_drain(
                _url: str,
                token: str,
                *,
                draining: bool = True,
                bearer_token: str | None = None,
            ) -> dict:
                del bearer_token
                return {
                    "drain": {
                        "draining": draining,
                        "token": token,
                        "admission_open": not draining,
                    }
                }

            def post_detach(
                _gateway_url: str,
                sandbox_id: str,
                **_kwargs,
            ) -> dict:
                detach_calls.append(sandbox_id)
                return {"sandbox": {"id": sandbox_id, "worker_state": "detached"}}

            with (
                patch.object(cli, "UCloudClient", SuccessfulStopClient),
                patch.object(cli, "_post_node_drain", side_effect=post_drain),
                patch.object(
                    cli,
                    "_post_gateway_sandbox_detach",
                    side_effect=post_detach,
                ),
            ):
                detached = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                    route_reservations={"owned": (route,)},
                )
                (intent,) = state.pending_drain_intents(deployment_id="prod-a")
                save_heartbeats(
                    heartbeat_file,
                    {"owned": heartbeat(token=intent.token, inventory=False)},
                )
                stopped = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                    route_reservations={},
                )

        self.assertEqual(detach_calls, [route.sandbox_id])
        self.assertEqual(terminate_calls, [("owned",)])
        self.assertEqual(detached["definitelyTerminatedJobIds"], [])
        self.assertTrue(
            detached["storage_native_detach_results"][0]["request_succeeded"]
        )
        self.assertEqual(stopped["drainReadyStopJobIds"], ["owned"])
        self.assertEqual(stopped["definitelyTerminatedJobIds"], ["owned"])

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

                config_file = write_ucloud_config(
                    root,
                    deployment_id="prod-a",
                    builder=replace(DeploymentConfig.default().builder, max_nodes=2),
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
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
            ["created-1", "created-2"],
        )
        self.assertEqual(len(submitted), 2)
        self.assertTrue(all(len(call[1]["items"]) == 1 for call in submitted))
        self.assertEqual(payload["preparedBuilderCount"], 2)
        self.assertEqual(payload["decision"]["actions"], [])
        self.assertEqual(
            [item["prepare_id"] for item in payload["consumedPreparedBuilders"]],
            ["builds-soon"],
        )
        self.assertEqual(payload["builderDecision"]["actions"][0]["kind"], "create")
        labels = {item["role"]: item["labels"] for item in payload["createIntents"]}[
            "builder"
        ]
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
                                    "resources": [
                                        {"type": "private_network", "id": "net-1"}
                                    ],
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

            config_file = write_ucloud_config(root, deployment_id="prod-a")
            output = io.StringIO()
            with redirect_stdout(output):
                result = cli.main(
                    [
                        "autoscaler",
                        "--config",
                        str(config_file),
                        "--jobs-file",
                        str(jobs_file),
                        "--once",
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
    def test_execute_runs_bootstrap_and_records_state(self) -> None:
        calls: list[dict] = []

        class FakeInitResult:
            returncode = 0

        def fake_run_init_over_ssh(
            ssh_command: str,
            script: str,
            *,
            timeout_seconds: int | None = None,
            private_key_file: str | None = None,
            known_hosts_file: str | None = None,
        ) -> FakeInitResult:
            calls.append(
                {
                    "ssh_command": ssh_command,
                    "script": script,
                    "timeout_seconds": timeout_seconds,
                    "private_key_file": private_key_file,
                    "known_hosts_file": known_hosts_file,
                }
            )
            return FakeInitResult()

        def fake_stage_package(
            _ssh_command: str,
            options,
            **_kwargs,
        ) -> VmInitPackageStageResult:
            local_path = Path(options.package_spec)
            return VmInitPackageStageResult(
                local_path=local_path,
                remote_path=f"/tmp/staged/{local_path.name}",
                command=("scp", str(local_path)),
                returncode=0,
                package_sha256="a" * 64,
            )

        original = cli.run_init_over_ssh
        original_stage = cli.stage_vm_init_package_over_ssh
        cli.run_init_over_ssh = fake_run_init_over_ssh
        cli.stage_vm_init_package_over_ssh = fake_stage_package
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                state_file = root / "control-state.sqlite"
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
                                        "resources": [
                                            {
                                                "type": "private_network",
                                                "id": "net-1",
                                            }
                                        ],
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

                config_file = write_ucloud_config(
                    root,
                    deployment_id="prod-a",
                    registry_private_ip="10.36.125.67",
                )
                (root / "heartbeat-token").write_text("SECRET", encoding="utf-8")
                (root / "node-control-token").write_text("NODESECRET", encoding="utf-8")
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--execute",
                            "--once",
                            "--output",
                            "json",
                        ]
                    )
                payload = json.loads(output.getvalue())
                state = ControlStateStore(state_file).load_bootstrap_records()
        finally:
            cli.run_init_over_ssh = original
            cli.stage_vm_init_package_over_ssh = original_stage

        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 1, payload)
        self.assertEqual(
            calls[0]["private_key_file"],
            str(root / "ssh/gateway-init"),
        )
        self.assertEqual(
            calls[0]["known_hosts_file"],
            str(root / "ssh-known-hosts" / "job-1"),
        )
        self.assertIn(
            "UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON='[\"ucloud-sandbox-registry:5000\"]'",
            calls[0]["script"],
        )
        self.assertIn(
            "UCLOUD_HOST_ALIASES_JSON='[\"ucloud-sandbox-registry=10.36.125.67\"]'",
            calls[0]["script"],
        )
        self.assertNotIn("OVERCOMMIT", calls[0]["script"])
        self.assertIn("UCLOUD_HEARTBEAT_BEARER_TOKEN=SECRET", calls[0]["script"])
        self.assertIn(
            "UCLOUD_PACKAGE_SPEC=/tmp/staged/builder-node-package.tar.gz",
            calls[0]["script"],
        )
        self.assertIn("serve-builder-agent", calls[0]["script"])
        self.assertNotIn("--execute-runtime", calls[0]["script"])
        self.assertEqual(payload["controlStateFile"], str(state_file))
        self.assertNotIn("heartbeatFile", payload)
        self.assertNotIn("bootstrapStateFile", payload)
        self.assertEqual(payload["bootstrapResults"][0]["status"], "succeeded")
        self.assertEqual(state["job-1"].status, "succeeded")
        self.assertEqual(state["job-1"].attempts, 1)

    @allow_fixture_mutations
    def test_execute_runs_bootstraps_concurrently_with_isolated_results(
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
            known_hosts_file: str | None = None,
        ) -> FakeInitResult:
            del timeout_seconds, private_key_file, known_hosts_file
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

        def fake_stage_package(
            _ssh_command: str,
            options,
            **_kwargs,
        ) -> VmInitPackageStageResult:
            local_path = Path(options.package_spec)
            return VmInitPackageStageResult(
                local_path=local_path,
                remote_path=f"/tmp/staged/{local_path.name}",
                command=("scp", str(local_path)),
                returncode=0,
                package_sha256="b" * 64,
            )

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
                    "resources": [{"type": "private_network", "id": "net-1"}],
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
        original_stage = cli.stage_vm_init_package_over_ssh
        cli.run_init_over_ssh = fake_run_init_over_ssh
        cli.stage_vm_init_package_over_ssh = fake_stage_package
        try:
            with TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                state_file = root / "control-state.sqlite"
                jobs_file = root / "jobs.json"
                jobs_file.write_text(
                    json.dumps({"items": [raw_job(index) for index in range(1, 4)]}),
                    encoding="utf-8",
                )

                config_file = write_ucloud_config(
                    root,
                    deployment_id="prod-a",
                    autoscaler_max_init_per_cycle=3,
                )
                (root / "heartbeat-token").write_text("HEARTBEAT", encoding="utf-8")
                (root / "node-control-token").write_text("NODE", encoding="utf-8")
                output = io.StringIO()
                with redirect_stdout(output):
                    result = cli.main(
                        [
                            "autoscaler",
                            "--config",
                            str(config_file),
                            "--jobs-file",
                            str(jobs_file),
                            "--execute",
                            "--once",
                            "--output",
                            "json",
                        ]
                    )
                payload = json.loads(output.getvalue())
                state = ControlStateStore(state_file).load_bootstrap_records()
                metric_events = [
                    event.to_dict()
                    for event in MetricsStore(root / "metrics.sqlite").load_events()
                ]
        finally:
            cli.run_init_over_ssh = original
            cli.stage_vm_init_package_over_ssh = original_stage

        self.assertEqual(result, 0)
        self.assertEqual(peak_active, 3, payload)
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
            [state[f"job-{index}"].status for index in range(1, 4)],
            ["succeeded", "failed", "succeeded"],
        )
        self.assertTrue(
            all(state[f"job-{index}"].attempts == 1 for index in range(1, 4))
        )
        init_metrics = [
            event for event in metric_events if event["kind"] == "vm_init_attempt"
        ]
        self.assertEqual(len(init_metrics), 3)
        self.assertEqual(
            {
                event["data"]["job_id"]: event["data"]["status"]
                for event in init_metrics
            },
            {"job-1": "succeeded", "job-2": "failed", "job-3": "succeeded"},
        )

    def test_unfenced_execute_fails_closed(self) -> None:
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
                heartbeat_file = root / "control-state.sqlite"
                save_heartbeats(
                    heartbeat_file,
                    {
                        job_id: NodeHeartbeat(
                            node_id=f"node-{job_id}",
                            job_id=job_id,
                            deployment_id="prod-a",
                            updated_at=utc_now(),
                            active_sandboxes=0,
                            total_resources=ResourceQuantity(
                                vcpu=2.0,
                                memory_mb=6144,
                                disk_mb=51200,
                            ),
                            resources_known=True,
                            capabilities=("disk-quota",),
                        )
                        for job_id in ("owned", "foreign")
                    },
                )
                config = ucloud_config(
                    project_id="project-1",
                    deployment_id="prod-a",
                    ucloud_session_file=str(root / "session.json"),
                    data_root=raw_dir,
                    policy=ScalePolicy(max_stop_per_cycle=2, scale_down_idle_seconds=0),
                )
                args = argparse.Namespace(
                    jobs_file=jobs_file,
                    control_state_file=heartbeat_file,
                    include_job=["foreign"],
                    execute=True,
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
                        demand=SandboxDemand(),
                    )
                remaining_heartbeats = ControlStateStore(
                    heartbeat_file
                ).load_heartbeats()
        finally:
            cli.UCloudClient = original_client

        self.assertEqual(terminated, [])
        self.assertIn("owned", remaining_heartbeats)
        self.assertIn("foreign", remaining_heartbeats)

    def test_fenced_stop_waits_for_matching_empty_gateway_heartbeat(self) -> None:
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
            heartbeat_file = root / "control-state.sqlite"

            def save_heartbeat(
                *,
                token: str = "",
                updated_at=None,
                reserved: ResourceQuantity = ResourceQuantity(),
            ) -> None:
                save_heartbeats(
                    heartbeat_file,
                    {
                        "owned": NodeHeartbeat(
                            node_id="node-owned",
                            job_id="owned",
                            deployment_id="prod-a",
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
                    },
                )

            save_heartbeat()
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
                policy=ScalePolicy(
                    max_stop_per_cycle=1,
                    scale_down_idle_seconds=0,
                    unreachable_stop_after_seconds=0,
                ),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                control_state_file=heartbeat_file,
                include_job=[],
                execute=True,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            def post_drain(
                _url: str,
                token: str,
                *,
                draining: bool = True,
                bearer_token=None,
            ) -> dict:
                del bearer_token
                drain_actions.append((token, draining))
                if len(drain_actions) == 1:
                    raise TimeoutError("drain request timed out")
                return {
                    "drain": {
                        "draining": draining,
                        "token": token,
                        "admission_open": not draining,
                    }
                }

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
                (intent,) = state.pending_drain_intents(deployment_id="prod-a")

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
                save_heartbeat()
                acknowledged = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                save_heartbeat()
                rearmed = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                (replacement_intent,) = state.pending_drain_intents(
                    deployment_id="prod-a"
                )
                save_heartbeat(token=replacement_intent.token)
                terminated = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )

        self.assertEqual(terminate_calls, [("owned",)])
        self.assertEqual(len({token for token, _draining in drain_actions}), 2)
        self.assertIn((intent.token, False), drain_actions)
        for blocked in (
            failed_request,
            stale,
            mismatch,
            reserved,
            acknowledged,
            rearmed,
        ):
            self.assertEqual(blocked["definitelyTerminatedJobIds"], [])
        self.assertEqual(terminated["drainReadyStopJobIds"], ["owned"])
        self.assertEqual(terminated["definitelyTerminatedJobIds"], ["owned"])
        self.assertEqual(mismatch["stopJobIds"], ["owned"])
        self.assertEqual(mismatch["drainingJobIds"], ["owned"])

    def test_unreachable_empty_node_uses_durable_stop_proof_without_drain(
        self,
    ) -> None:
        terminate_calls: list[tuple[str, ...]] = []

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
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        deployment_id="prod-a",
                        updated_at=utc_now() - timedelta(hours=1),
                        active_sandboxes=0,
                        node_url="http://node-owned:8090",
                        agent_version=package_version(),
                        capabilities=("disk-quota",),
                        inventory_complete=True,
                    )
                },
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
                policy=ScalePolicy(
                    max_stop_per_cycle=1,
                    unreachable_stop_after_seconds=1800,
                ),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                control_state_file=heartbeat_file,
                include_job=[],
                execute=True,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            with patch.object(cli, "UCloudClient", SuccessfulStopClient), patch.object(
                cli,
                "_post_node_drain",
                side_effect=AssertionError("unreachable node must not be drained"),
            ):
                result = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )

        self.assertEqual(terminate_calls, [("owned",)])
        self.assertEqual(result["unreachableReadyStopJobIds"], ["owned"])
        self.assertEqual(result["drainReadyStopJobIds"], [])
        self.assertEqual(result["drainIntents"], [])
        self.assertEqual(result["bootstrapIntents"], [])
        self.assertEqual(result["definitelyTerminatedJobIds"], ["owned"])

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
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        deployment_id="prod-a",
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
                        resources_known=True,
                    )
                },
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                control_state_file=heartbeat_file,
                include_job=[],
                execute=True,
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
                    demand=SandboxDemand(pending_resources=ResourceQuantity(vcpu=1)),
                    provider_state=state,
                    provider_mutations_allowed=True,
                )
                acknowledged = cli.run_reconcile_cycle(
                    config,
                    args,
                    demand=SandboxDemand(pending_resources=ResourceQuantity(vcpu=1)),
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
        self.assertEqual(acknowledged["drainingJobIds"], [])
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
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": NodeHeartbeat(
                        node_id="node-owned",
                        job_id="owned",
                        deployment_id="prod-a",
                        updated_at=utc_now(),
                        active_sandboxes=0,
                        idle_since=utc_now() - timedelta(minutes=10),
                        node_url="http://node-owned:8090",
                        agent_version=package_version(),
                        capabilities=("disk-quota",),
                    )
                },
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                control_state_file=heartbeat_file,
                include_job=[],
                execute=True,
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
                    (intent,) = state.pending_drain_intents(deployment_id="prod-a")
                    save_heartbeats(
                        heartbeat_file,
                        {
                            "owned": NodeHeartbeat(
                                node_id="node-owned",
                                job_id="owned",
                                deployment_id="prod-a",
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
                        },
                    )
                    second = cli.run_reconcile_cycle(
                        config,
                        args,
                        demand=SandboxDemand(),
                        provider_state=state,
                        provider_mutations_allowed=True,
                    )
                    save_heartbeats(
                        heartbeat_file,
                        {
                            "owned": NodeHeartbeat(
                                node_id="node-owned",
                                job_id="owned",
                                deployment_id="prod-a",
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
                        },
                    )
                    third = cli.run_reconcile_cycle(
                        config,
                        args,
                        demand=SandboxDemand(),
                        provider_state=state,
                        provider_mutations_allowed=True,
                    )
                    save_heartbeats(
                        heartbeat_file,
                        {
                            "owned": NodeHeartbeat(
                                node_id="node-owned",
                                job_id="owned",
                                deployment_id="prod-a",
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
                        },
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
            remaining = ControlStateStore(heartbeat_file).load_heartbeats()

        self.assertEqual(terminate_calls, [("owned",), ("owned",)])
        self.assertEqual(first["providerOperationResults"], [])
        self.assertEqual(first["definitelyTerminatedJobIds"], [])
        self.assertEqual(second["providerOperationResults"][-1]["state"], "uncertain")
        self.assertEqual(second["definitelyTerminatedJobIds"], [])
        self.assertEqual(third["providerOperationResults"][0]["state"], "retry")
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
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
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
                },
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=raw_dir,
            )
            args = argparse.Namespace(
                jobs_file=jobs_file,
                control_state_file=heartbeat_file,
                include_job=[],
                execute=False,
                pending_image_builds=0,
                max_builder_nodes=0,
                seed_prefix="test",
            )

            result = cli.run_reconcile_cycle(
                config,
                args,
                demand=SandboxDemand(),
            )
            remaining_heartbeats = ControlStateStore(heartbeat_file).load_heartbeats()

        self.assertEqual(result["prunedFinalHeartbeats"], ["finished-node"])
        self.assertIn("finished-node", remaining_heartbeats)


if __name__ == "__main__":
    unittest.main()
