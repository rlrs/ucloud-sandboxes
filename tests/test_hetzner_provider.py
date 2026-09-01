import argparse
import os
import unittest
from unittest.mock import patch

from ucloud_sandboxes import cli
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.models import InstancePhase
from ucloud_sandboxes.providers.base import InstanceCreateIntent, ProviderConfiguration
from ucloud_sandboxes.providers.hetzner import (
    HetznerClient,
    HetznerCreateProfile,
    HetznerError,
    HetznerHttpError,
    HetznerProvider,
    HetznerSettings,
    MANAGED_SERVER_LABEL_SELECTOR,
)


def server_payload(
    *,
    server_id=42,
    status="running",
    network_id=1001,
    private_ip="10.20.0.42",
):
    return {
        "id": server_id,
        "name": "ucloud-sandbox-node-seed-1",
        "status": status,
        "created": "2026-08-12T08:10:11+00:00",
        "server_type": {
            "name": "cx43",
            "category": "shared",
            "cores": 8,
            "memory": 16.0,
            "disk": 160,
        },
        "primary_disk_size": 160,
        "image": {
            "id": 9001,
            "type": "snapshot",
            "name": "sandbox-node-v2",
            "description": "sandbox node v2",
            "os_flavor": "ubuntu",
            "os_version": "24.04",
        },
        "location": {"name": "hel1"},
        "private_net": [{"network": network_id, "ip": private_ip, "alias_ips": []}],
        "public_net": {
            "ipv4": {"ip": "192.0.2.42"},
            "ipv6": {"ip": "2001:db8::/64"},
        },
        "labels": {
            "ucloud-sandboxes/node": "true",
            "ucloud-sandboxes/reconcile": "true",
            "ucloud-sandboxes/deployment": "prod-a",
        },
    }


class FakeHetznerClient:
    def __init__(self):
        self.servers = []
        self.create_results = []
        self.delete_results = []
        self.calls = []

    def list_servers(self, *, label_selector):
        self.calls.append(("list", label_selector))
        return list(self.servers)

    def retrieve_server(self, server_id):
        self.calls.append(("retrieve", server_id))
        return self.servers[0]

    def create_server(self, payload):
        self.calls.append(("create", payload))
        result = self.create_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def delete_server(self, server_id):
        self.calls.append(("delete", server_id))
        result = self.delete_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def provider(
    client=None,
    *,
    image=9001,
    enable_ipv4=False,
    enable_private_egress=False,
):
    profile = HetznerCreateProfile(
        server_type="cx43",
        image=image,
        location="hel1",
        network_id=1001,
        ssh_key_ids=(11,),
        firewall_ids=(22,),
        placement_group_id=33,
        enable_ipv4=enable_ipv4,
        enable_private_egress=enable_private_egress,
        private_dns_servers=("1.1.1.1", "8.8.8.8"),
    )
    return HetznerProvider(
        "sandbox-project",
        client=client,
        api_token_env="HETZNER_API_KEY",
        api_base_url="https://api.hetzner.cloud/v1",
        ssh_user="root",
        sandbox_profile=profile,
        builder_profile=profile,
    )


def create_intent(name="ucloud-sandbox-node-seed-1"):
    return InstanceCreateIntent(
        seed="seed-1",
        role="sandbox",
        name=name,
        node_id="sandbox-node-seed-1",
        node_url="http://sandbox-node-seed-1:8090",
        labels={
            "ucloud-sandboxes/node": "true",
            "ucloud-sandboxes/reconcile": "true",
            "ucloud-sandboxes/provider-operation": "provider-1234abcd",
        },
    )


class HetznerSettingsTests(unittest.TestCase):
    def test_exact_settings_support_snapshot_ids_and_safe_network_defaults(self):
        settings = HetznerSettings.from_provider(
            ProviderConfiguration(
                kind="hetzner",
                scope_id="sandbox-project",
                settings={
                    "network_id": 1001,
                    "sandbox_image": 9001,
                    "builder_image": "ubuntu-24.04",
                    "ssh_key_ids": [11],
                    "firewall_ids": [22],
                },
            )
        )

        self.assertEqual(settings.sandbox_image, 9001)
        self.assertEqual(settings.builder_image, "ubuntu-24.04")
        self.assertFalse(settings.enable_ipv4)
        self.assertFalse(settings.enable_ipv6)
        self.assertFalse(settings.enable_private_egress)

    def test_settings_reject_unknown_missing_and_wrong_typed_fields(self):
        valid = {
            "network_id": 1,
            "sandbox_image": 1,
            "builder_image": 2,
            "ssh_key_ids": [11],
        }
        cases = (
            {key: value for key, value in valid.items() if key != "network_id"},
            {**valid, "volume_size": 100},
            {**valid, "enable_ipv4": "false"},
            {**valid, "sandbox_image": True},
            {**valid, "ssh_key_ids": []},
            {**valid, "enable_ipv4": True},
            {**valid, "api_base_url": "http://api.example.test/v1"},
            {
                **valid,
                "enable_ipv4": True,
                "firewall_ids": [22],
                "enable_private_egress": True,
            },
            {**valid, "private_dns_servers": ["not-an-ip"]},
        )
        for settings in cases:
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                HetznerSettings.from_provider(
                    ProviderConfiguration(
                        kind="hetzner",
                        scope_id="sandbox-project",
                        settings=settings,
                    )
                )

    def test_deployment_config_and_cli_recognize_builtin_hetzner_provider(self):
        raw = DeploymentConfig.default(scope_id="old-ucloud-project").to_dict()
        raw["provider"] = {
            "kind": "hetzner",
            "scope_id": "sandbox-project",
            "network_id": 1001,
            "sandbox_image": 9001,
            "builder_image": 9002,
            "ssh_key_ids": [11],
        }

        config = DeploymentConfig.from_dict(raw)
        compute = cli.compute_provider_from_args(argparse.Namespace(), config)

        self.assertIsInstance(compute, HetznerProvider)
        self.assertEqual(compute.scope_id, "sandbox-project")


class HetznerClientTests(unittest.TestCase):
    def test_token_is_loaded_lazily(self):
        client = HetznerClient(api_token_env="ABSENT_HETZNER_TOKEN")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(HetznerError, "ABSENT_HETZNER_TOKEN"):
                client.request_json("GET", "/servers")

    def test_server_list_reads_every_page(self):
        class PaginatedClient(HetznerClient):
            def __init__(self):
                self.calls = []

            def request_json(self, method, path, *, params=None, json_body=None):
                del method, path, json_body
                self.calls.append(dict(params or {}))
                if params["page"] == 1:
                    return {
                        "servers": [{"id": 1}],
                        "meta": {"pagination": {"next_page": 2}},
                    }
                return {
                    "servers": [{"id": 2}],
                    "meta": {"pagination": {"next_page": None}},
                }

        client = PaginatedClient()
        servers = client.list_servers(label_selector="owned=true")

        self.assertEqual([item["id"] for item in servers], [1, 2])
        self.assertEqual([call["page"] for call in client.calls], [1, 2])
        self.assertEqual(client.calls[0]["label_selector"], "owned=true")

    def test_server_ids_cannot_escape_the_api_path(self):
        client = HetznerClient()
        with self.assertRaisesRegex(HetznerError, "invalid Hetzner server id"):
            client.retrieve_server("1/actions/poweroff")


class HetznerProviderTests(unittest.TestCase):
    def test_inventory_decode_and_bootstrap_use_configured_private_network(self):
        client = FakeHetznerClient()
        client.servers = [server_payload()]
        compute = provider(client)

        instances = compute.list_instances()
        instance = instances[0]
        access = compute.bootstrap_access(instance)

        self.assertEqual(client.calls[0], ("list", MANAGED_SERVER_LABEL_SELECTOR))
        self.assertEqual(instance.phase, InstancePhase.RUNNING)
        self.assertEqual(instance.hostname, "10.20.0.42")
        self.assertTrue(compute.instance_is_eligible(instance))
        self.assertIsNone(compute.unreachable_lease_expiry_loss)
        self.assertTrue(access.runnable)
        self.assertEqual(access.command, "ssh root@10.20.0.42")

    def test_powered_off_server_is_lost_and_missing_private_ip_refreshes(self):
        compute = provider(FakeHetznerClient())
        off = compute.decode_instance(server_payload(status="off"))
        running_without_network = compute.decode_instance(
            server_payload(status="running", network_id=9999)
        )

        self.assertEqual(off.phase, InstancePhase.LOST)
        self.assertIsNone(compute.destructive_instance_loss(off))
        access = compute.bootstrap_access(running_without_network)
        self.assertFalse(access.runnable)
        self.assertTrue(access.refresh_recommended)
        self.assertFalse(compute.instance_is_eligible(running_without_network))

    def test_render_create_exposes_only_selected_networking(self):
        private = provider(FakeHetznerClient()).render_create_request(
            [create_intent()]
        )["servers"][0]
        self.assertEqual(private["image"], 9001)
        self.assertEqual(private["networks"], [1001])
        self.assertEqual(
            private["public_net"], {"enable_ipv4": False, "enable_ipv6": False}
        )
        self.assertFalse({"volumes", "firewalls", "root_password"} & private.keys())

        public = provider(FakeHetznerClient(), enable_ipv4=True).render_create_request(
            [create_intent()]
        )["servers"][0]
        self.assertTrue(public["public_net"]["enable_ipv4"])
        self.assertEqual(public["firewalls"], [{"firewall": 22}])

        egress = provider(
            FakeHetznerClient(), enable_private_egress=True
        ).render_create_request([create_intent()])["servers"][0]
        self.assertFalse(egress["public_net"]["enable_ipv4"])
        self.assertNotIn("firewalls", egress)
        for directive in (
            "169.254.169.254/32",
            "ip -4 route replace default",
            "nameserver 1.1.1.1",
        ):
            self.assertIn(directive, egress["user_data"])

    def test_render_rejects_invalid_names_labels_and_duplicates(self):
        compute = provider(FakeHetznerClient())
        invalid_label = create_intent()
        invalid_label = invalid_label.with_labels({"valid": "contains a space"})
        for intents in (
            [create_intent("INVALID")],
            [invalid_label],
            [create_intent(), create_intent()],
        ):
            with self.subTest(intents=intents), self.assertRaises(ValueError):
                compute.render_create_request(intents)

    def test_create_accepts_and_never_persists_root_password(self):
        client = FakeHetznerClient()
        client.create_results = [
            {
                "server": {"id": 42, "name": "node", "status": "initializing"},
                "action": {"id": 7, "command": "create_server", "status": "running"},
                "root_password": "DO-NOT-PERSIST",
            }
        ]
        compute = provider(client)

        result = compute.create(compute.render_create_request([create_intent()]))

        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.instance_ids, ("42",))
        self.assertNotIn("DO-NOT-PERSIST", str(result.response))
        self.assertNotIn("root_password", str(result.response))

    def test_create_classifies_deterministic_ambiguous_and_partial_failures(self):
        bad_request = HetznerHttpError(
            "POST", "/servers", 400, {"error": {"code": "invalid_input"}}
        )
        unavailable = HetznerHttpError(
            "POST", "/servers", 503, {"error": {"code": "unavailable"}}
        )
        for failure, expected in (
            (bad_request, "rejected"),
            (unavailable, "uncertain"),
        ):
            client = FakeHetznerClient()
            client.create_results = [failure]
            result = provider(client).create({"servers": [{"name": "node"}]})
            self.assertEqual(result.status, expected)

        client = FakeHetznerClient()
        client.create_results = [
            {"server": {"id": 41}, "action": {"id": 1}},
            bad_request,
        ]
        result = provider(client).create(
            {"servers": [{"name": "node-1"}, {"name": "node-2"}]}
        )
        self.assertEqual(result.status, "uncertain")
        self.assertEqual(result.instance_ids, ("41",))

    def test_terminate_is_idempotent_for_not_found_and_classifies_errors(self):
        client = FakeHetznerClient()
        client.delete_results = [
            {"action": {"id": 9, "command": "delete_server"}},
            HetznerHttpError(
                "DELETE", "/servers/43", 404, {"error": {"code": "not_found"}}
            ),
        ]
        result = provider(client).terminate(("42", "43"))

        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.instance_ids, ("42", "43"))

        client = FakeHetznerClient()
        client.delete_results = [
            HetznerHttpError(
                "DELETE", "/servers/42", 403, {"error": {"code": "forbidden"}}
            )
        ]
        self.assertEqual(provider(client).terminate(("42",)).status, "rejected")


if __name__ == "__main__":
    unittest.main()
