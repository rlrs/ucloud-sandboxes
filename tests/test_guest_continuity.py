"""VM readiness/silence never grants authority to destroy a UCloud guest."""

from dataclasses import replace
from datetime import timedelta
import unittest
import json
from urllib.request import Request, urlopen
from unittest.mock import MagicMock, patch

from tests.test_cli import (
    autoscaler_args,
    owned_heartbeat,
    owned_node_job,
    reconcile,
    sandbox_route,
    temporary_root,
    ucloud_config,
    write_jobs,
)
from ucloud_sandboxes import cli
from ucloud_sandboxes.autoscaler_state import AutoscalerStateStore
from ucloud_sandboxes.control_state import ControlStateStore, QUARANTINE_REASON
from ucloud_sandboxes.models import (
    InstancePhase,
    SandboxInventoryEntry,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.providers.ucloud import UCloudProvider
from ucloud_sandboxes.providers.ucloud.models import instance_from_payload
from ucloud_sandboxes.registry import heartbeat_to_dict
from tests.test_control_plane import _gateway_server, _running_server
from ucloud_sandboxes.routing import RoutingStore


def suspended_job(state="RUNNING"):
    payload = owned_node_job()
    payload["status"] = {"state": state, "startedAt": 1_700_000_100_000}
    payload["updates"] = [{"state": s} for s in ("RUNNING", "SUSPENDED", state)]
    return instance_from_payload(payload)


def continuity_pair():
    route = sandbox_route(
        "sandbox", node_id="node-owned", job_id="owned", node_epoch="boot-a"
    )
    heartbeat = owned_heartbeat(
        node_epoch="boot-a",
        inventory_complete=True,
        active_sandboxes=1,
        received_at=utc_now(),
        inventory=(
            SandboxInventoryEntry(
                sandbox_id=route.sandbox_id,
                generation=route.generation,
                operation_id=route.create_operation_id,
                spec_hash=route.spec_hash,
                state="running",
                resources=route.resources,
            ),
        ),
    )
    return heartbeat, route


class GuestContinuityTests(unittest.TestCase):
    def test_same_guest_recovers_from_historical_suspension(self):
        with temporary_root() as root:
            store = ControlStateStore(root / "control-state.sqlite")
            heartbeat, route = continuity_pair()
            store.receive_heartbeat(heartbeat)
            with patch.object(
                cli, "_probe_unreachable_node", return_value=(heartbeat, False)
            ):
                jobs, heartbeats = cli._quarantine_unverified_guests(
                    [suspended_job()],
                    store.load_heartbeats(),
                    control_state=store,
                    policy=ScalePolicy(),
                    deployment_id="prod-a",
                    route_reservations={"owned": (route,)},
                    execution_authorized=True,
                    bearer_token="test-token",
                )
            self.assertEqual(jobs[0].phase, InstancePhase.RUNNING)
            self.assertTrue(heartbeats["owned"].admission_open)
            self.assertNotIn(QUARANTINE_REASON, store.get_heartbeat("owned").labels)

    def test_suspended_or_unreachable_guest_keeps_routes_and_cannot_stop(self):
        for state, route_only in (
            ("SUSPENDED", False),
            ("RUNNING", False),
            ("RUNNING", True),
        ):
            with (
                self.subTest(state=state, route_only=route_only),
                temporary_root() as root,
            ):
                heartbeat, route = continuity_pair()
                heartbeat = replace(
                    heartbeat,
                    updated_at=utc_now() - timedelta(hours=2),
                    received_at=utc_now() - timedelta(hours=2),
                )
                if route_only:
                    heartbeat = replace(heartbeat, active_sandboxes=0, inventory=())
                control = ControlStateStore(root / "control-state.sqlite")
                control.receive_heartbeat(heartbeat)
                routing = RoutingStore(root / "routes.sqlite")
                routing.upsert_sandbox(route)
                payload = owned_node_job()
                payload["status"] = {"state": state, "startedAt": 1_700_000_100_000}
                jobs = write_jobs(root, payload)
                config = ucloud_config(
                    data_root=str(root),
                    deployment_id="prod-a",
                    project_id="project-1",
                    policy=ScalePolicy(
                        max_create_per_cycle=0,
                        scale_down_idle_seconds=0,
                        unreachable_stop_after_seconds=1,
                    ),
                )
                provider_state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
                with (
                    patch.object(
                        cli, "_probe_unreachable_node", return_value=(None, True)
                    ),
                    patch.object(cli, "UCloudClient") as client,
                ):
                    result = reconcile(
                        config,
                        autoscaler_args(jobs, control.path),
                        provider_state,
                        route_reservations={"owned": (route,)},
                        sandbox_routes=(route,),
                    )
                self.assertEqual(result["quarantined_job_ids"], ["owned"])
                self.assertEqual(result["destructive_node_loss_job_ids"], [])
                self.assertEqual(result["lost_sandbox_ids"], [])
                self.assertEqual(provider_state.list_operations(kind="stop"), [])
                self.assertIsNotNone(routing.get_sandbox(route.sandbox_id))
                self.assertFalse(control.get_heartbeat("owned").admission_open)
                client.return_value.terminate_jobs.assert_not_called()

    def test_quarantine_survives_push_restart_and_rejects_obsolete_recovery(self):
        with temporary_root() as root:
            path = root / "control-state.sqlite"
            store = ControlStateStore(path)
            heartbeat, _ = continuity_pair()
            store.receive_heartbeat(heartbeat)
            store.quarantine_node("owned", "unverified")
            newer = replace(
                heartbeat, activity_epoch=10, updated_at=utc_now(), labels={}
            )
            self.assertFalse(store.receive_heartbeat(newer).stored.admission_open)
            restarted = ControlStateStore(path)
            self.assertFalse(restarted.get_heartbeat("owned").admission_open)
            self.assertFalse(restarted.recover_quarantined_node(heartbeat))
            self.assertTrue(restarted.recover_quarantined_node(newer))
            self.assertTrue(restarted.get_heartbeat("owned").admission_open)
            # A worker cannot invent controller-owned quarantine metadata either.
            store.receive_heartbeat(
                replace(newer, labels={QUARANTINE_REASON: "spoofed"})
            )
            self.assertNotIn(QUARANTINE_REASON, store.get_heartbeat("owned").labels)

    def test_incomplete_or_wrong_incarnation_inventory_cannot_reopen_placement(self):
        heartbeat, route = continuity_pair()
        entry = heartbeat.inventory[0]
        invalid = [
            replace(heartbeat, inventory_complete=False),
            replace(heartbeat, inventory=()),
            replace(heartbeat, node_epoch="new-boot"),
            replace(heartbeat, inventory=(replace(entry, generation=2),)),
            replace(heartbeat, inventory=(replace(entry, operation_id="other"),)),
            replace(heartbeat, inventory=(replace(entry, spec_hash="b" * 64),)),
            replace(heartbeat, inventory=(entry, entry)),
            replace(heartbeat, active_sandbox_creates=1),
        ]
        for fresh in invalid:
            with self.subTest(fresh=fresh):
                self.assertFalse(
                    cli._guest_continuity_matches(heartbeat, fresh, (route,))
                )
        self.assertTrue(cli._guest_continuity_matches(heartbeat, heartbeat, (route,)))

    def test_obsolete_destructive_stop_cannot_replay_even_after_uncertain_call(self):
        for phase in ("prepared", "uncertain", "accepted"):
            for reason in (
                "post_start_suspension",
                "ucloud_unreachable_lease_expired",
                "ucloud_unreachable_retirement",
            ):
                with self.subTest(phase=phase, reason=reason), temporary_root() as root:
                    state = AutoscalerStateStore(root / "autoscaler.sqlite")
                    operation = state.prepare_operation(
                        intent_key="old-stop",
                        kind="stop",
                        role="sandbox",
                        deployment_id="prod-a",
                        target_job_ids=("owned",),
                        request={
                            "destructiveNodeLoss": True,
                            "lossReason": reason,
                            "type": "bulk",
                            "items": [{"id": "owned"}],
                        },
                    )
                    if phase != "prepared":
                        state.begin_provider_call(operation.operation_id)
                    if phase == "accepted":
                        state.mark_operation_accepted(
                            operation.operation_id,
                            response={"responses": [{"id": "owned"}]},
                        )
                    provider = MagicMock(spec=UCloudProvider)
                    provider.destructive_instance_losses = ()
                    cli._reconcile_provider_operation_inventory(
                        provider_state=state,
                        provider=provider,
                        jobs=[suspended_job()],
                        execution_authorized=True,
                        execute=True,
                        telemetry=MagicMock(),
                    )
                    actual = state.get_operation(operation.operation_id)
                    self.assertEqual(
                        actual.state, "accepted" if phase == "accepted" else "failed"
                    )
                    if phase == "uncertain":
                        self.assertTrue(actual.response["providerCallStarted"])
                    provider.terminate.assert_not_called()

    def test_old_boot_cleanup_preserves_new_boot_route(self):
        with temporary_root() as root:
            routing = RoutingStore(root / "routes.sqlite")
            _, old = continuity_pair()
            new = replace(
                old,
                sandbox_id="new-sandbox",
                spec={"id": "new-sandbox"},
                node_epoch="boot-b",
            )
            routing.upsert_sandbox(old)
            routing.upsert_sandbox(new)
            removed = routing.delete_sandboxes_for_jobs_with_error(
                ("owned",), terminal_error="node_lost", retired_node_epoch="boot-a"
            )
            self.assertEqual([route.sandbox_id for route in removed], [old.sandbox_id])
            self.assertIsNone(routing.get_sandbox(old.sandbox_id))
            self.assertEqual(routing.get_sandbox(new.sandbox_id).node_epoch, "boot-b")

    def test_authenticated_new_boot_cannot_inherit_old_routes(self):
        with temporary_root() as root:
            heartbeat, route = continuity_pair()
            # The route and heartbeat must bind the same canonical endpoint.
            route = replace(route, node_url=heartbeat.node_url)
            control = ControlStateStore(root / "control-state.sqlite")
            control.receive_heartbeat(heartbeat)
            control.quarantine_node("owned", "readiness")
            routing = RoutingStore(root / "routes.sqlite")
            routing.upsert_sandbox(route)
            gateway = _gateway_server(
                root,
                routing_file=root / "routes.sqlite",
                deployment_id="prod-a",
                heartbeat_bearer_token="heartbeat-secret",
            )
            with _running_server(gateway) as url:
                # Even an identical inventory must not adopt the old incarnation.
                fresh = replace(heartbeat, node_epoch="boot-b")
                req = Request(
                    url + "/v1/nodes/heartbeat",
                    method="POST",
                    data=json.dumps(heartbeat_to_dict(fresh)).encode(),
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": "Bearer heartbeat-secret",
                    },
                )
                with urlopen(req, timeout=5) as response:
                    self.assertEqual(response.status, 200)
            self.assertIsNone(routing.get_sandbox(route.sandbox_id))
            stored = control.get_heartbeat("owned")
            self.assertEqual(stored.node_epoch, "boot-b")
            self.assertFalse(stored.admission_open)
            self.assertIn("boot-a", stored.retired_node_epochs)

    def test_fresh_empty_suspended_guest_cannot_finish_existing_drain(self):
        with temporary_root() as root:
            control = ControlStateStore(root / "control-state.sqlite")
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
            intent = state.prepare_drain_intent(
                deployment_id="prod-a", job_id="owned", role="sandbox"
            )
            control.upsert_heartbeat(
                owned_heartbeat(
                    node_epoch="boot-a",
                    inventory_complete=True,
                    draining=True,
                    admission_open=False,
                    drain_token=intent.token,
                    activity_epoch=7,
                    drain_activity_epoch=7,
                    idle_since=utc_now() - timedelta(hours=1),
                )
            )
            payload = owned_node_job()
            payload["status"] = {"state": "SUSPENDED", "startedAt": 1_700_000_100_000}
            jobs = write_jobs(root, payload)
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                data_root=str(root),
                policy=ScalePolicy(max_create_per_cycle=0, scale_down_idle_seconds=0),
            )
            with (
                patch.object(cli, "UCloudClient") as client,
                patch.object(cli, "_probe_unreachable_node") as probe,
                patch.object(cli, "_post_node_drain", return_value={}),
            ):
                result = reconcile(config, autoscaler_args(jobs, control.path), state)
            probe.assert_not_called()
            client.return_value.terminate_jobs.assert_not_called()
            self.assertEqual(result["drainReadyStopJobIds"], [])
            self.assertEqual(result["quarantined_job_ids"], ["owned"])
            self.assertEqual(state.list_operations(kind="stop"), [])
