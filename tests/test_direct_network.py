from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
import json
import unittest
import threading

from ucloud_sandboxes.direct_network import (
    DirectNetworkError,
    DirectNetworkManager,
    DirectNetworkTcpEgress,
)


class DirectNetworkManagerTests(unittest.TestCase):
    def manager(self, root: Path) -> DirectNetworkManager:
        return DirectNetworkManager(
            root / "network-slots.json",
            namespace_root=root / "netns",
        )

    def test_rule_snapshot_requires_exact_predicates_and_table(self) -> None:
        raw = (
            "*filter\n:FORWARD ACCEPT [0:0]\n"
            "-A FORWARD -s 100.96.0.0/16 -d 10.36.0.2/32 "
            "-p tcp -m tcp --dport 8092 -j ACCEPT\nCOMMIT\n"
            "*nat\n-A POSTROUTING -s 100.96.0.0/16 -j MASQUERADE\nCOMMIT\n"
        )
        with patch(
            "ucloud_sandboxes.direct_network.subprocess.run",
            return_value=Mock(returncode=0, stdout=raw),
        ):
            snapshot = DirectNetworkManager._iptables_snapshot()
        self.assertIsNotNone(snapshot)
        key = DirectNetworkManager._iptables_rule_key
        rule = ("iptables", "-C", "FORWARD", "-s", "100.96.0.0/16",
                "-d", "10.36.0.2/32", "-p", "tcp", "--dport", "8092", "-j", "ACCEPT")
        self.assertIn(key(rule), snapshot)
        self.assertNotIn(key((*rule[:-1], "DROP")), snapshot)
        self.assertNotIn(key((*rule, "-m", "comment", "--comment", "extra")), snapshot)
        self.assertNotIn(key(tuple("8093" if x == "8092" else x for x in rule)), snapshot)
        self.assertNotIn(key(("iptables", "-t", "nat", *rule[1:])), snapshot)
        self.assertIn(key(("iptables", "-t", "nat", "-C", "POSTROUTING",
                           "-s", "100.96.0.0/16", "-j", "MASQUERADE")), snapshot)

    def test_rule_snapshot_rejects_failed_or_incomplete_reads(self) -> None:
        cases = [
            Mock(returncode=1, stdout="*filter\nCOMMIT\n"),
            Mock(returncode=0, stdout="*filter\n-A INPUT -j DROP\n"),
            Mock(returncode=0, stdout="-A INPUT -j DROP\nCOMMIT\n"),
            Mock(returncode=0, stdout='*filter\n-A INPUT --comment "broken\nCOMMIT\n'),
        ]
        for result in cases:
            with self.subTest(result=result), patch(
                "ucloud_sandboxes.direct_network.subprocess.run", return_value=result
            ):
                self.assertIsNone(DirectNetworkManager._iptables_snapshot())
        with patch("ucloud_sandboxes.direct_network.subprocess.run", side_effect=FileNotFoundError):
            self.assertIsNone(DirectNetworkManager._iptables_snapshot())

    def test_missing_snapshot_rule_uses_original_check_and_repair(self) -> None:
        with TemporaryDirectory() as raw:
            manager = self.manager(Path(raw).resolve())
            manager.runner = Mock()
            check = ("iptables", "-C", "INPUT", "-j", "DROP")
            install = ("iptables", "-I", "INPUT", "1", "-j", "DROP")
            with patch("ucloud_sandboxes.direct_network.subprocess.run",
                       return_value=Mock(returncode=1)) as run:
                manager._ensure_iptables(check, install, snapshot=set())
            self.assertEqual(run.call_args.args[0], check)
            manager.runner.assert_called_once_with(install)

    def test_host_reconciliation_reads_policy_fresh_each_time(self) -> None:
        with TemporaryDirectory() as raw:
            manager = self.manager(Path(raw).resolve())
            manager.runner = Mock()
            checks = []
            with patch.object(manager, "_iptables_snapshot", return_value=set()), patch.object(
                manager, "_ensure_iptables", side_effect=lambda check, _install, **_kwargs: checks.append(check)
            ):
                manager._ensure_host_rules()
            all_rules = {manager._iptables_rule_key(check) for check in checks}
            with patch.object(manager, "_iptables_snapshot", side_effect=[all_rules, set()]) as snapshot, patch(
                "ucloud_sandboxes.direct_network.subprocess.run", return_value=Mock(returncode=0)
            ) as run:
                manager._ensure_host_rules()
                run.assert_not_called()
                manager._ensure_host_rules()
                self.assertEqual(run.call_count, len(checks))
                self.assertEqual(snapshot.call_count, 2)

    def test_kernel_work_is_independent_but_cleanup_keeps_its_slot(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            first = self.manager(root)
            second = self.manager(root)
            entered = threading.Event()
            unblock = threading.Event()
            release_started = threading.Event()
            cleaned = threading.Event()

            def configure(lease):
                if lease.sandbox_id == "a":
                    entered.set()
                    if not unblock.wait(5):
                        raise TimeoutError("test did not release namespace setup")

            def release_a():
                release_started.set()
                second.release("a", 1)

            with patch.object(first, "_ensure_host_rules"), patch.object(
                second, "_ensure_host_rules"
            ), patch.object(first, "_ensure_kernel_lease", side_effect=configure), patch.object(
                second, "_ensure_kernel_lease"
            ), patch.object(second, "_cleanup_kernel_lease", side_effect=lambda _: cleaned.set()), ThreadPoolExecutor(max_workers=3) as pool:
                creating = pool.submit(first.ensure, "a", 1)
                try:
                    self.assertTrue(entered.wait(5))
                    other = pool.submit(second.ensure, "b", 1).result(timeout=2)
                    deleting = pool.submit(release_a)
                    self.assertTrue(release_started.wait(5))
                    self.assertFalse(cleaned.wait(0.05))
                    third = second.ensure("c", 1)
                    self.assertEqual((other.slot, third.slot), (2, 3))
                finally:
                    unblock.set()
                self.assertEqual(creating.result(timeout=5).slot, 1)
                deleting.result(timeout=5)
                replacement = second.ensure("d", 1)
                self.assertEqual(replacement.slot, 1)
                self.assertTrue(cleaned.is_set())
                self.assertIsNone(first.lease("a", 1))
            lock_directory = first.lock_path.with_name(first.lock_path.name + ".leases")
            self.assertEqual(len(list(lock_directory.glob("*.lock"))), 4)

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
        hostname = DirectNetworkTcpEgress.parse("Relay.Internal.:8092")
        self.assertEqual(hostname.endpoint(), "relay.internal:8092")
        self.assertTrue(hostname.is_dynamic)
        for value in (
            "10.36.136.151",
            "10.36.136.151:0",
            "10.36.136.151:65536",
            "[fd00::1]:8092",
            "-relay.internal:8092",
            "relay..internal:8092",
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

            allows = [
                command
                for command in commands
                if "10.36.136.151/32" in command and "ACCEPT" in command
            ]
            deny_index = next(
                index
                for index, command in enumerate(commands)
                if "10.0.0.0/8" in command and "DROP" in command
            )
            self.assertEqual(len(allows), 1)
            self.assertGreater(commands.index(allows[0]), deny_index)

    def test_dns_egress_handoff_adds_new_rule_before_removing_old(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            events: list[tuple[str, tuple[str, ...]]] = []
            addresses = [["10.36.136.151"], ["10.36.144.34"]]
            manager = DirectNetworkManager(
                root / "network-slots.json",
                namespace_root=root / "netns",
                allowed_tcp_egress=("relay.internal:8092",),
                runner=lambda command: events.append(("add", tuple(command))),
                resolver=lambda _host: addresses.pop(0),
            )
            with (
                patch(
                    "ucloud_sandboxes.direct_network.subprocess.run",
                    return_value=Mock(returncode=1),
                ),
                patch.object(
                    manager,
                    "_run_best_effort",
                    side_effect=lambda command: events.append(
                        ("remove", tuple(command))
                    ),
                ),
            ):
                manager._ensure_host_rules()
                manager._ensure_host_rules()

            new_allow = next(
                index
                for index, (kind, command) in enumerate(events)
                if kind == "add" and "10.36.144.34/32" in command
            )
            old_remove = next(
                index
                for index, (kind, command) in enumerate(events)
                if kind == "remove" and "10.36.136.151/32" in command
            )
            self.assertLess(new_allow, old_remove)

            replayed: list[tuple[str, ...]] = []
            reopened = DirectNetworkManager(
                root / "network-slots.json",
                namespace_root=root / "netns",
                allowed_tcp_egress=("relay.internal:8092",),
                runner=lambda command: replayed.append(tuple(command)),
                resolver=lambda _host: (_ for _ in ()).throw(OSError("DNS down")),
            )
            with patch(
                "ucloud_sandboxes.direct_network.subprocess.run",
                return_value=Mock(returncode=1),
            ):
                reopened._ensure_host_rules()
            self.assertTrue(any("10.36.144.34/32" in command for command in replayed))

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
            self.assertTrue(cleanup.called)

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

            rendered = [" ".join(command) for command in commands]
            self.assertTrue(
                any(f"address replace {lease.guest_ip}/31" in item for item in rendered)
            )
            self.assertTrue(
                any(
                    f"route replace default via {lease.host_ip}" in item
                    for item in rendered
                )
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
