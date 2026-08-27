import argparse
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import fields, replace
from datetime import timedelta
from functools import wraps
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from urllib.error import HTTPError
from unittest.mock import MagicMock, patch

from ucloud_sandboxes import cli
from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.autoscaler_state import (
    AutoscalerStateStore,
)
from ucloud_sandboxes.cli import (
    find_ucloud_ssh_key,
    read_public_ssh_key_file,
)
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.managed_registry import RegistryTag, RegistryUsageStore
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxDemand,
    SandboxInventoryEntry,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.routing import (
    RoutingStore,
    SandboxRoute,
)
from ucloud_sandboxes.providers.ucloud.api import UCloudError
from ucloud_sandboxes.providers.ucloud.config import UCloudSettings


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
        "sandbox-api-token",
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


def write_jobs(root: Path, *jobs: dict) -> Path:
    path = root / "jobs.json"
    path.write_text(json.dumps({"items": list(jobs)}), encoding="utf-8")
    return path


def owned_node_job(*, agent_version: bool = False) -> dict:
    labels = {
        "ucloud-sandboxes/node": "true",
        "ucloud-sandboxes/deployment": "prod-a",
    }
    if agent_version:
        labels["ucloud-sandboxes/agent-version"] = package_version()
    return {
        "id": "owned",
        "owner": {"project": "project-1"},
        "specification": {
            "name": "ucloud-sandbox-node-owned",
            "application": {"name": "vm-ubuntu", "version": "24.04"},
            "product": {
                "id": "cpu-amd-zen5-2-vcpu",
                "category": "cpu-amd-zen5",
            },
            "labels": labels,
        },
        "status": {"state": "RUNNING"},
    }


def owned_heartbeat(**values) -> NodeHeartbeat:
    fields = {
        "node_id": "node-owned",
        "job_id": "owned",
        "deployment_id": "prod-a",
        "updated_at": utc_now(),
        "active_sandboxes": 0,
        "node_url": "http://node-owned:8090",
        "agent_version": package_version(),
        "capabilities": ("disk-quota",),
    }
    fields.update(values)
    return NodeHeartbeat(**fields)


def autoscaler_args(jobs_file: Path, heartbeat_file: Path) -> argparse.Namespace:
    return argparse.Namespace(
        jobs_file=jobs_file,
        control_state_file=heartbeat_file,
        include_job=[],
        execute=True,
        pending_image_builds=0,
        max_builder_nodes=0,
        seed_prefix="test",
    )


@contextmanager
def temporary_root():
    with TemporaryDirectory() as raw_dir:
        yield Path(raw_dir)


def reconcile(
    config: DeploymentConfig,
    args: argparse.Namespace,
    state: AutoscalerStateStore,
    *,
    demand: SandboxDemand | None = None,
    **values,
) -> dict:
    return cli.run_reconcile_cycle(
        config,
        args,
        demand=SandboxDemand() if demand is None else demand,
        provider_state=state,
        provider_mutations_allowed=True,
        **values,
    )


def allow_fixture_mutations(test):
    """Run provider-journal unit cases against deterministic fixtures."""

    @wraps(test)
    def wrapped(*args, **kwargs):
        with patch.object(cli, "reject_mutating_jobs_fixture", return_value=None):
            return test(*args, **kwargs)

    return wrapped


class CliTests(unittest.TestCase):
    def test_dashboard_policy_exposes_every_scale_policy_field(self) -> None:
        policy = ScalePolicy()

        exposed = cli.dashboard_scale_policy_to_dict(policy)

        self.assertEqual(set(exposed), {field.name for field in fields(policy)})
        self.assertEqual(
            exposed["default_node_resources"],
            policy.default_node_resources.to_dict(),
        )
        self.assertEqual(exposed["warm_resources"], policy.warm_resources.to_dict())

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

        unsafe_exec = HTTPError(
            "https://gateway.example",
            409,
            "Conflict",
            {},
            io.BytesIO(
                b'{"error":"sandbox has active exec/file activity that cannot '
                b'survive park; use SDK start_agent()"}'
            ),
        )
        with (
            patch.object(
                cli,
                "_post_bounded_json",
                side_effect=unsafe_exec,
            ) as post,
            patch.object(cli.time, "sleep") as sleep,
            self.assertRaisesRegex(RuntimeError, "start_agent"),
        ):
            cli._post_gateway_sandbox_lifecycle(
                "https://gateway.example",
                "secret",
                relay_request,
                action="park",
            )
        self.assertEqual(post.call_count, 1)
        sleep.assert_not_called()

    def test_autoscaler_execute_rejects_jobs_fixture_before_provider_calls(
        self,
    ) -> None:
        class ForbiddenClient:
            def __init__(self, *_args, **_kwargs) -> None:
                raise AssertionError("provider client must not be constructed")

        with temporary_root() as root:
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

        with temporary_root() as root:
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

        with temporary_root() as root:
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
                data_root=str(root),
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

        with temporary_root() as root:
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

    def test_read_public_ssh_key_file_validates_single_openssh_key(self) -> None:
        with temporary_root() as root:
            key_file = root / "gateway-init.pub"
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
            with temporary_root() as root:
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

    @allow_fixture_mutations
    def test_autoscaler_loop_preserves_pending_signal_created_during_cycle(
        self,
    ) -> None:
        submitted: list[tuple[str, dict]] = []

        original_client = cli.UCloudClient
        try:
            with temporary_root() as root:
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
            with temporary_root() as root:
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
            with temporary_root() as root:
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

    def test_reconcile_detaches_park_before_provider_stop(self) -> None:
        terminate_calls: list[tuple[str, ...]] = []
        detach_calls: list[str] = []
        delete_calls: list[str] = []

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
        delete_route = sandbox_route(
            sandbox_id="delete-pending-elsewhere",
            node_id="other-node",
            job_id="other-job",
            state="parked",
            delete_operation_id="delete-pending-elsewhere-operation",
        )
        with temporary_root() as root:
            jobs_file = write_jobs(root, owned_node_job(agent_version=True))
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
                return owned_heartbeat(
                    idle_since=utc_now() - timedelta(minutes=10),
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
                data_root=str(root),
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
                autoscaler_max_pending_delete_retries_per_cycle=1,
                autoscaler_max_storage_native_detaches_per_cycle=1,
            )
            args = autoscaler_args(jobs_file, heartbeat_file)
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

            def replay_delete(
                _gateway_url: str,
                sandbox_id: str,
                **_kwargs,
            ) -> dict:
                delete_calls.append(sandbox_id)
                return {"deleted": {"sandbox_id": sandbox_id}}

            with (
                patch.object(cli, "UCloudClient", SuccessfulStopClient),
                patch.object(cli, "_post_node_drain", side_effect=post_drain),
                patch.object(
                    cli,
                    "_post_gateway_sandbox_detach",
                    side_effect=post_detach,
                ),
                patch.object(
                    cli,
                    "_delete_gateway_sandbox",
                    side_effect=replay_delete,
                ),
            ):
                detached = reconcile(
                    config,
                    args,
                    state,
                    route_reservations={"owned": (route,)},
                    sandbox_routes=(delete_route,),
                )
                (intent,) = state.pending_drain_intents(deployment_id="prod-a")
                save_heartbeats(
                    heartbeat_file,
                    {"owned": heartbeat(token=intent.token, inventory=False)},
                )
                stopped = reconcile(
                    config,
                    args,
                    state,
                    route_reservations={},
                )

        self.assertEqual(detach_calls, [route.sandbox_id])
        self.assertEqual(delete_calls, [delete_route.sandbox_id])
        self.assertEqual(terminate_calls, [("owned",)])
        self.assertEqual(detached["definitelyTerminatedJobIds"], [])
        self.assertTrue(
            detached["storage_native_detach_results"][0]["request_succeeded"]
        )
        self.assertEqual(stopped["drainReadyStopJobIds"], ["owned"])
        self.assertEqual(stopped["definitelyTerminatedJobIds"], ["owned"])

    def test_reconcile_replays_pending_delete_then_scales_down(self) -> None:
        delete_calls: list[str] = []
        terminate_calls: list[tuple[str, ...]] = []

        class SuccessfulStopClient:
            def __init__(self, _session_store) -> None:
                pass

            def terminate_jobs(
                self, _project_id: str, job_ids: tuple[str, ...]
            ) -> dict:
                terminate_calls.append(tuple(job_ids))
                return {"responses": [{"id": job_id} for job_id in job_ids]}

        route = sandbox_route(
            sandbox_id="delete-pending",
            node_id="node-owned",
            job_id="owned",
            state="parked",
            delete_operation_id="delete-pending-operation",
        )
        with temporary_root() as root:
            jobs_file = write_jobs(root, owned_node_job(agent_version=True))
            heartbeat_file = root / "control-state.sqlite"

            def heartbeat(*, token: str = "", inventory=True) -> NodeHeartbeat:
                entries = (
                    (
                        SandboxInventoryEntry(
                            sandbox_id=route.sandbox_id,
                            state="parked",
                            generation=route.generation,
                            operation_id=route.create_operation_id,
                            spec_hash=route.spec_hash,
                        ),
                    )
                    if inventory
                    else ()
                )
                return owned_heartbeat(
                    idle_since=utc_now() - timedelta(minutes=10),
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
                data_root=str(root),
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = autoscaler_args(jobs_file, heartbeat_file)
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            def replay_delete(
                _gateway_url: str,
                sandbox_id: str,
                **_kwargs,
            ) -> dict:
                delete_calls.append(sandbox_id)
                return {"deleted": {"sandbox_id": sandbox_id}}

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

            with (
                patch.object(cli, "UCloudClient", SuccessfulStopClient),
                patch.object(
                    cli,
                    "_delete_gateway_sandbox",
                    side_effect=replay_delete,
                ),
                patch.object(cli, "_post_node_drain", side_effect=post_drain),
            ):
                replayed = reconcile(
                    config,
                    args,
                    state,
                    route_reservations={"owned": (route,)},
                    sandbox_routes=(route,),
                )
                save_heartbeats(
                    heartbeat_file,
                    {"owned": heartbeat(inventory=False)},
                )
                draining = reconcile(
                    config,
                    args,
                    state,
                    route_reservations={},
                )
                (intent,) = state.pending_drain_intents(deployment_id="prod-a")
                save_heartbeats(
                    heartbeat_file,
                    {"owned": heartbeat(token=intent.token, inventory=False)},
                )
                stopped = reconcile(
                    config,
                    args,
                    state,
                    route_reservations={},
                )

        self.assertEqual(delete_calls, [route.sandbox_id])
        self.assertTrue(replayed["pending_delete_results"][0]["request_succeeded"])
        self.assertEqual(replayed["requestedStopJobIds"], [])
        self.assertEqual(draining["stopJobIds"], ["owned"])
        self.assertEqual(stopped["drainReadyStopJobIds"], ["owned"])
        self.assertEqual(terminate_calls, [("owned",)])

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
            with temporary_root() as root:
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
                    data_root=str(root),
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

        with temporary_root() as root:
            jobs_file = write_jobs(root, owned_node_job())
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
                        "owned": owned_heartbeat(
                            updated_at=updated_at or utc_now(),
                            idle_since=utc_now() - timedelta(minutes=10),
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
                data_root=str(root),
                policy=ScalePolicy(
                    max_stop_per_cycle=1,
                    scale_down_idle_seconds=0,
                    unreachable_stop_after_seconds=0,
                ),
            )
            args = autoscaler_args(jobs_file, heartbeat_file)
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

            with (
                patch.object(cli, "UCloudClient", SuccessfulStopClient),
                patch.object(cli, "_post_node_drain", side_effect=post_drain),
            ):
                failed_request = reconcile(config, args, state)
                (intent,) = state.pending_drain_intents(deployment_id="prod-a")

                save_heartbeat(
                    token=intent.token,
                    updated_at=utc_now() - timedelta(hours=1),
                )
                stale = reconcile(config, args, state)
                save_heartbeat(token="wrong-token")
                mismatch = reconcile(config, args, state)
                save_heartbeat(
                    token=intent.token,
                    reserved=ResourceQuantity(vcpu=1),
                )
                reserved = reconcile(config, args, state)
                save_heartbeat()
                acknowledged = reconcile(config, args, state)
                save_heartbeat()
                rearmed = reconcile(config, args, state)
                (replacement_intent,) = state.pending_drain_intents(
                    deployment_id="prod-a"
                )
                save_heartbeat(token=replacement_intent.token)
                terminated = reconcile(config, args, state)

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

        with temporary_root() as root:
            jobs_file = write_jobs(root, owned_node_job())
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": owned_heartbeat(
                        updated_at=utc_now() - timedelta(hours=1),
                        inventory_complete=True,
                    )
                },
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=str(root),
                policy=ScalePolicy(
                    max_stop_per_cycle=1,
                    unreachable_stop_after_seconds=1800,
                ),
            )
            args = autoscaler_args(jobs_file, heartbeat_file)
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")

            with (
                patch.object(cli, "UCloudClient", SuccessfulStopClient),
                patch.object(
                    cli,
                    "_post_node_drain",
                    side_effect=AssertionError("unreachable node must not be drained"),
                ),
            ):
                result = reconcile(config, args, state)

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

        with temporary_root() as root:
            jobs_file = write_jobs(root, owned_node_job())
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": owned_heartbeat(
                        idle_since=utc_now() - timedelta(minutes=10),
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
                data_root=str(root),
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = autoscaler_args(jobs_file, heartbeat_file)
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

            with (
                patch.object(cli, "UCloudClient", SuccessfulStopClient),
                patch.object(cli, "_post_node_drain", side_effect=post_drain),
            ):
                initial = reconcile(config, args, state)
                rising = reconcile(
                    config,
                    args,
                    state,
                    demand=SandboxDemand(pending_resources=ResourceQuantity(vcpu=1)),
                )
                acknowledged = reconcile(
                    config,
                    args,
                    state,
                    demand=SandboxDemand(pending_resources=ResourceQuantity(vcpu=1)),
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

        with temporary_root() as root:
            jobs_file = write_jobs(root, owned_node_job(agent_version=True))
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {
                    "owned": owned_heartbeat(
                        idle_since=utc_now() - timedelta(minutes=10),
                    )
                },
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=str(root),
                policy=ScalePolicy(max_stop_per_cycle=1, scale_down_idle_seconds=0),
            )
            args = autoscaler_args(jobs_file, heartbeat_file)
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
            original_client = cli.UCloudClient
            cli.UCloudClient = AmbiguousStopClient
            try:
                with patch.object(cli, "_post_node_drain", return_value={}):
                    first = reconcile(config, args, state)
                    (intent,) = state.pending_drain_intents(deployment_id="prod-a")
                    save_heartbeats(
                        heartbeat_file,
                        {
                            "owned": owned_heartbeat(
                                idle_since=utc_now() - timedelta(minutes=10),
                                draining=True,
                                admission_open=False,
                                drain_token=intent.token,
                                inventory_complete=True,
                                activity_epoch=4,
                                drain_activity_epoch=4,
                            )
                        },
                    )
                    second = reconcile(config, args, state)
                    save_heartbeats(
                        heartbeat_file,
                        {
                            "owned": owned_heartbeat(
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
                    third = reconcile(config, args, state)
                    save_heartbeats(
                        heartbeat_file,
                        {
                            "owned": owned_heartbeat(
                                draining=True,
                                admission_open=False,
                                drain_token=intent.token,
                                inventory_complete=True,
                                activity_epoch=6,
                                drain_activity_epoch=6,
                            )
                        },
                    )
                    fourth = reconcile(config, args, state)
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
