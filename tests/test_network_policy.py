from dataclasses import asdict, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

from ucloud_sandboxes.config import SandboxPoolConfig
from ucloud_sandboxes.control_plane import _sandbox_required_capabilities
from ucloud_sandboxes.direct_network import DirectNetworkError, DirectNetworkManager
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder, DirectOciConfigError
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.network_policy import SandboxNetworkPolicy
from ucloud_sandboxes.relay_network import NetworkRelay, relay_policy_rules
from ucloud_sandboxes.sandbox import (
    SandboxSpec,
    SandboxSshSpec,
    sandbox_spec_fingerprint,
)


RELAY = SandboxNetworkPolicy.relay_only()


class NetworkPolicyContractTests(unittest.TestCase):
    def test_strict_contract_and_legacy_fingerprint(self):
        spec = SandboxSpec(id="test", image="image", memory_mb=128, cpus=1, disk_mb=128)
        self.assertNotIn("network_policy", spec.to_dict())
        restricted = replace(spec, network_policy=RELAY)
        restricted.validate()
        self.assertEqual(SandboxSpec.from_dict(restricted.to_dict()), restricted)
        self.assertNotEqual(
            sandbox_spec_fingerprint(spec), sandbox_spec_fingerprint(restricted)
        )
        self.assertEqual(
            _sandbox_required_capabilities(restricted.to_dict()),
            ("network-policy-relay-v1:default",),
        )
        for invalid in (
            None,
            [],
            "relay",
            {"egress": "allow"},
            {"egress": "relay"},
            {"egress": "relay", "relay": "../bad"},
            {"relay": "default"},
            {"egress": "relay", "relay": "default", "allow": ["*"]},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                SandboxSpec.from_dict({**spec.to_dict(), "network_policy": invalid})
        for other in (
            replace(restricted, network="none"),
            replace(restricted, dns_servers=("8.8.8.8",)),
            replace(restricted, ssh=SandboxSshSpec(enabled=True)),
        ):
            with self.assertRaises(ValueError):
                other.validate()

    def test_trusted_configuration_is_validated(self):
        config = SandboxPoolConfig.from_dict(
            {
                **asdict(SandboxPoolConfig()),
                "direct_network_allow_tcp": [],
                "network_relays": {"default": "Relay.Example:443"},
            }
        )
        self.assertEqual(config.network_relays, {"default": "relay.example:443"})
        for raw in (
            [],
            {"default": "x:0"},
            {"a\naccept": "x:80"},
            {"default": "https://x"},
        ):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                SandboxPoolConfig.from_dict(
                    {
                        **asdict(SandboxPoolConfig()),
                        "direct_network_allow_tcp": [],
                        "network_relays": raw,
                    }
                )

    def test_create_and_migration_pass_policy_to_network_owner(self):
        manager = Mock()
        manager.ensure.return_value = SimpleNamespace(
            namespace_path=Path("/run/netns/test"), guest_ip="100.96.0.3"
        )
        provisioner = object.__new__(DirectSandboxProvisioner)
        provisioner.network_manager = manager
        registration = SimpleNamespace(
            spec=SandboxSpec(id="test", image="image", network_policy=RELAY),
            sandbox_id="test",
            sandbox_generation=2,
        )
        provisioner.ensure_network(registration)
        self.assertEqual(manager.ensure.call_args.kwargs["network_policy"], RELAY)
        from ucloud_sandboxes.storage_native_migration import (
            MIGRATION_CONNECTION_POLICY_DISCONNECT,
        )

        provisioner._migration_network_namespace(
            registration,
            source_guest_ip="100.96.0.5",
            connection_policy=MIGRATION_CONNECTION_POLICY_DISCONNECT,
        )
        self.assertEqual(manager.ensure.call_args.kwargs["network_policy"], RELAY)
        self.assertEqual(
            manager.ensure.call_args.kwargs["avoid_guest_ips"], ("100.96.0.5",)
        )


class RelayNetworkLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.addresses = ["10.36.0.2"]
        self.nft = Mock()
        self.manager = DirectNetworkManager(
            self.root / "state.json",
            namespace_root=self.root / "netns",
            network_relays={"default": "relay.example:443"},
            resolver=lambda _: self.addresses,
            nft_runner=self.nft,
            runner=Mock(),
        )
        for name in (
            "_ensure_host_rules",
            "_ensure_kernel_lease",
            "_cleanup_kernel_lease",
        ):
            patcher = patch.object(self.manager, name)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(self.manager, "_command_ok", return_value=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def create(self):
        return self.manager.ensure("test", 1, network_policy=RELAY)

    def test_policy_is_durable_before_interfaces_and_immutable(self):
        def at_setup(lease):
            self.assertTrue(self.nft.called)
            self.assertEqual(
                self.manager._load()["policies"]["test\x001"], RELAY.to_dict()
            )

        self.manager._ensure_kernel_lease.side_effect = at_setup
        self.create()
        with self.assertRaisesRegex(DirectNetworkError, "immutable"):
            self.manager.ensure("test", 1)
        with self.assertRaisesRegex(ValueError, "not configured"):
            self.manager.ensure(
                "other", 1, network_policy=SandboxNetworkPolicy.relay_only("missing")
            )

    def test_firewall_failure_never_activates_interface_and_can_retry(self):
        self.nft.side_effect = OSError("nft failed")
        with self.assertRaises(OSError):
            self.create()
        self.manager._ensure_kernel_lease.assert_not_called()
        self.assertIn("test\x001", self.manager._load()["policies"])
        self.nft.side_effect = None
        self.create()
        self.manager._ensure_kernel_lease.assert_called_once()

    def test_dns_changes_are_atomic_and_failure_installs_deny_all(self):
        lease = self.create()
        first = self.nft.call_args.args[0]
        self.assertIn("dnat ip to 10.36.0.2:443", first)
        self.addresses = ["10.36.0.3"]
        self.manager.refresh_tcp_egress()
        updated = self.nft.call_args.args[0]
        self.assertNotIn("10.36.0.2", updated)
        self.assertIn("dnat ip to 10.36.0.3:443", updated)
        self.nft.reset_mock()
        self.manager.refresh_tcp_egress()
        self.nft.assert_not_called()
        self.addresses = []
        self.manager.refresh_tcp_egress()
        blocked = self.nft.call_args.args[0]
        self.assertNotIn("dnat ip to", blocked)
        self.assertIn(f'iifname "{lease.host_interface}" drop', blocked)
        self.addresses = ["10.36.0.4"]
        self.manager.refresh_tcp_egress()
        self.assertIn("dnat ip to 10.36.0.4:443", self.nft.call_args.args[0])

    def test_restart_restores_guards_before_legacy_accepts(self):
        self.create()
        manager = DirectNetworkManager(
            self.root / "state.json",
            network_relays={"default": "relay.example:443"},
            resolver=lambda _: ["10.36.0.9"],
            nft_runner=self.nft,
            runner=Mock(),
        )
        with (
            patch.object(manager, "_command_ok", return_value=False),
            patch.object(manager, "_ensure_host_rules") as host,
        ):

            def legacy():
                self.assertIn("dnat ip to 10.36.0.9:443", self.nft.call_args.args[0])

            host.side_effect = legacy
            manager.reconcile()
        self.assertIn("dnat ip to 10.36.0.9:443", self.nft.call_args.args[0])

    def test_release_retains_policy_and_slot_until_kernel_cleanup_succeeds(self):
        lease = self.create()
        with patch.object(self.manager, "_command_ok", return_value=True):
            with self.assertRaisesRegex(DirectNetworkError, "interface exists"):
                self.manager.release("test", 1)
        self.assertIsNotNone(self.manager.lease("test", 1))
        self.manager.release("test", 1)
        self.assertIsNone(self.manager.lease("test", 1))
        self.assertIn(
            f"delete table inet ucloud_relay_{lease.slot}", self.nft.call_args.args[0]
        )
        self.manager._ensure_kernel_lease.side_effect = None
        other = self.manager.ensure("other", 1)
        self.assertEqual(lease.slot, other.slot)
        self.assertEqual(self.manager._load()["policies"], {})

    def test_forbidden_dns_answers_fail_closed(self):
        for address in (
            "127.0.0.1",
            "169.254.169.254",
            "224.0.0.1",
            "0.0.0.0",
            "100.96.0.3",
            "::1",
        ):
            self.addresses = [address]
            self.manager._relay_resolution.clear()
            with self.subTest(address=address), self.assertRaises(DirectNetworkError):
                self.create()
            self.assertNotIn("dnat ip to", self.nft.call_args.args[0])
        self.manager._ensure_kernel_lease.assert_not_called()

    def test_removing_configuration_revokes_existing_lease(self):
        self.create()
        other = DirectNetworkManager(
            self.root / "state.json", nft_runner=self.nft, runner=Mock()
        )
        with self.assertRaisesRegex(DirectNetworkError, "missing"):
            other.reconcile()
        self.assertNotIn("dnat ip to", self.nft.call_args.args[0])
        self.assertIsNotNone(other.lease("test", 1))

    def test_host_interface_source_and_both_ip_families_are_guarded(self):
        lease = self.create()
        rules = relay_policy_rules(
            lease, NetworkRelay.parse("default", "relay.example:443"), ["10.36.0.2"]
        )
        self.assertIn("table inet", rules)
        self.assertIn("hook prerouting priority -150", rules)
        self.assertIn(f"ip saddr {lease.guest_ip}", rules)
        self.assertIn(f'iifname "{lease.host_interface}" drop', rules)
        self.assertIn("hook input", rules)
        self.assertIn("hook output", rules)
        self.assertNotIn("udp", rules)


class RelayNetworkFilesTests(unittest.TestCase):
    def test_hostname_mapping_preserves_tls_name_and_avoids_public_dns(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            (root / "etc").mkdir()
            outside = root / "outside"
            outside.write_text("untouched")
            (root / "etc/hosts").symlink_to(outside)
            builder = object.__new__(DirectOciConfigBuilder)
            spec = SandboxSpec(id="test", image="image", network_policy=RELAY)
            builder.prepare_network_files(
                root, spec=spec, relay_hosts={"relay.example": "198.18.0.1"}
            )
            self.assertEqual(outside.read_text(), "untouched")
            self.assertIn("198.18.0.1 relay.example", (root / "etc/hosts").read_text())
            self.assertNotIn("nameserver", (root / "etc/resolv.conf").read_text())
            with self.assertRaises(DirectOciConfigError):
                builder.prepare_network_files(root, spec=spec)
