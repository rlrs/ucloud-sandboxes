from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
import json
import unittest

from ucloud_sandboxes.direct_network import (
    DirectNetworkError,
    DirectNetworkManager,
    DirectNetworkTcpEgress,
    NETWORK_MTU,
)


class DirectNetworkManagerTests(unittest.TestCase):
    def manager(self, root: Path) -> DirectNetworkManager:
        return DirectNetworkManager(
            root / "network-slots.json",
            namespace_root=root / "netns",
        )

    def test_allocates_unique_durable_slots_and_stable_namespace(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            manager = self.manager(root)
            with (
                patch.object(manager, "_ensure_host_rules"),
                patch.object(manager, "_ensure_kernel_lease"),
            ):
                first = manager.ensure("sandbox-a", 1)
                second = manager.ensure("sandbox-b", 1)
                replay = manager.ensure("sandbox-a", 1)

            self.assertEqual(first.slot, 1)
            self.assertEqual(second.slot, 2)
            self.assertEqual(replay, first)
            self.assertEqual(first.host_ip, "100.96.0.2")
            self.assertEqual(first.guest_ip, "100.96.0.3")
            self.assertTrue(first.namespace_path.is_absolute())

            reopened = self.manager(root)
            self.assertEqual(reopened.lease("sandbox-a", 1), first)

    def test_validates_exact_tcp_egress_endpoints(self) -> None:
        self.assertEqual(
            DirectNetworkTcpEgress.parse("10.36.136.151:8092").endpoint(),
            "10.36.136.151:8092",
        )
        for value in (
            "relay.internal:8092",
            "10.36.136.151",
            "10.36.136.151:0",
            "10.36.136.151:65536",
            "[fd00::1]:8092",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                DirectNetworkTcpEgress.parse(value)

    def test_exact_tcp_egress_is_installed_above_private_denies(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            commands: list[tuple[str, ...]] = []
            manager = DirectNetworkManager(
                root / "network-slots.json",
                namespace_root=root / "netns",
                allowed_tcp_egress=(
                    "10.36.136.151:8092",
                    "10.36.136.151:8092",
                ),
                runner=lambda command: commands.append(tuple(command)),
            )
            with patch(
                "ucloud_sandboxes.direct_network.subprocess.run",
                return_value=Mock(returncode=1),
            ):
                manager._ensure_host_rules()

            deny = (
                "iptables", "-I", "FORWARD", "1",
                "-s", "100.96.0.0/16",
                "-d", "10.0.0.0/8",
                "-j", "DROP",
            )
            allow = (
                "iptables", "-I", "FORWARD", "1",
                "-s", "100.96.0.0/16",
                "-d", "10.36.136.151/32",
                "-p", "tcp",
                "--dport", "8092",
                "-j", "ACCEPT",
            )
            self.assertIn(deny, commands)
            self.assertEqual(commands.count(allow), 1)
            self.assertGreater(commands.index(allow), commands.index(deny))

    def test_release_cleans_kernel_before_reusing_slot(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            manager = self.manager(root)
            with (
                patch.object(manager, "_ensure_host_rules"),
                patch.object(manager, "_ensure_kernel_lease"),
            ):
                first = manager.ensure("sandbox-a", 4)
            with patch.object(manager, "_cleanup_kernel_lease") as cleanup:
                manager.release("sandbox-a", 4)
            cleanup.assert_called_once_with(first)

            with (
                patch.object(manager, "_ensure_host_rules"),
                patch.object(manager, "_ensure_kernel_lease"),
            ):
                replacement = manager.ensure("sandbox-b", 9)
            self.assertEqual(replacement.slot, first.slot)

    def test_migration_avoids_source_guest_ip_even_on_empty_node(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            manager = self.manager(root)
            with (
                patch.object(manager, "_ensure_host_rules"),
                patch.object(manager, "_ensure_kernel_lease"),
            ):
                migrated = manager.ensure(
                    "sandbox-a",
                    4,
                    avoid_guest_ips=("100.96.0.3",),
                )
                replay = manager.ensure(
                    "sandbox-a",
                    4,
                    avoid_guest_ips=("100.96.0.3",),
                )

            self.assertEqual(migrated.slot, 2)
            self.assertEqual(migrated.guest_ip, "100.96.0.5")
            self.assertEqual(replay, migrated)

    def test_migration_fails_closed_on_conflicting_existing_lease(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            manager = self.manager(root)
            with (
                patch.object(manager, "_ensure_host_rules"),
                patch.object(manager, "_ensure_kernel_lease"),
            ):
                existing = manager.ensure("sandbox-a", 4)
                with self.assertRaisesRegex(
                    DirectNetworkError,
                    "forbidden guest IP",
                ):
                    manager.ensure(
                        "sandbox-a",
                        4,
                        avoid_guest_ips=(existing.guest_ip,),
                    )

    def test_rejects_double_allocated_durable_state(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            state = root / "network-slots.json"
            state.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "leases": {
                            "sandbox-a\u00001": 1,
                            "sandbox-b\u00001": 1,
                        },
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                DirectNetworkError,
                "double-allocates",
            ):
                self.manager(root).lease("sandbox-a", 1)

    def test_applies_ucloud_path_mtu_to_both_veth_ends(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            commands: list[tuple[str, ...]] = []
            manager = DirectNetworkManager(
                root / "network-slots.json",
                namespace_root=root / "netns",
                runner=lambda command: commands.append(tuple(command)),
            )
            lease = manager._lease("sandbox-a", 1, 1)

            manager._ensure_lease_mtu(lease)

            self.assertEqual(
                commands,
                [
                    (
                        "ip", "link", "set", "dev", lease.host_interface,
                        "mtu", str(NETWORK_MTU),
                    ),
                    (
                        "ip", "netns", "exec", lease.namespace, "ip", "link",
                        "set", "dev", "eth0", "mtu", str(NETWORK_MTU),
                    ),
                ],
            )

    def test_reconciles_complete_existing_lease_before_restore(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            commands: list[tuple[str, ...]] = []
            manager = DirectNetworkManager(
                root / "network-slots.json",
                namespace_root=root / "netns",
                runner=lambda command: commands.append(tuple(command)),
            )
            lease = manager._lease("sandbox-a", 1, 1)
            lease.namespace_path.parent.mkdir(parents=True)
            lease.namespace_path.touch()
            with patch.object(manager, "_command_ok", return_value=True):
                manager._ensure_kernel_lease(lease)

            self.assertIn(
                (
                    "ip", "netns", "exec", lease.namespace, "ip", "address",
                    "replace", f"{lease.guest_ip}/31", "dev", "eth0",
                ),
                commands,
            )
            self.assertIn(
                (
                    "ip", "netns", "exec", lease.namespace, "ip", "route",
                    "replace", "default", "via", lease.host_ip, "dev", "eth0",
                ),
                commands,
            )

    def test_recreates_lease_when_guest_veth_was_consumed(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            commands: list[tuple[str, ...]] = []
            manager = DirectNetworkManager(
                root / "network-slots.json",
                namespace_root=root / "netns",
                runner=lambda command: commands.append(tuple(command)),
            )
            lease = manager._lease("sandbox-a", 1, 1)
            lease.namespace_path.parent.mkdir(parents=True)
            lease.namespace_path.touch()
            with (
                patch.object(manager, "_command_ok", side_effect=(True, False)),
                patch.object(manager, "_cleanup_kernel_lease") as cleanup,
            ):
                manager._ensure_kernel_lease(lease)

            cleanup.assert_called_once_with(lease)
            self.assertIn(("ip", "netns", "add", lease.namespace), commands)


if __name__ == "__main__":
    unittest.main()
