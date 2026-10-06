"""The pre-created netns+veth pool: refill, hand-off, exhaustion and crashes."""

from concurrent.futures import ThreadPoolExecutor
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock, current_thread
from time import monotonic, sleep
from unittest.mock import patch
import json
import os
import shutil
import stat
import subprocess
import sys
import unittest

from ucloud_sandboxes.direct_network import DirectNetworkError, DirectNetworkManager


class FakeKernel:
    """Namespaces are files naming an id, as bind mounts name one nsfs inode.

    A namespace lives while a name refers to it; dropping its last name
    destroys it with its veth pair. ``links`` maps each host interface to
    the namespace holding its ``eth0`` peer.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.links: dict[str, int] = {}
        self.configured: set[str] = set()
        self.commands: list[tuple[str, tuple[str, ...]]] = []
        self.block_pool_add: Event | None = None
        self.fail_namespace: str | None = None
        self._ids = count(1)
        self._guard = Lock()

    def install(self, manager: DirectNetworkManager) -> DirectNetworkManager:
        manager.runner = self.run
        manager.ip_batch_runner = self.batch
        manager._command_ok = self.command_ok
        manager._run_best_effort = self.best_effort
        manager._attach_namespace = self.attach
        manager._detach_namespace = self.detach
        manager._interface_present = self.links.__contains__
        manager._ensure_host_rules = lambda: None
        return manager

    def namespace(self, name: str) -> int | None:
        try:
            return int((self.root / name).read_text())
        except FileNotFoundError:
            return None

    def names(self) -> set[str]:
        return {path.name for path in self.root.iterdir()} if self.root.exists() else set()

    def ip_by(self, thread_name: str) -> list[tuple[str, ...]]:
        return [command for name, command in self.commands if name == thread_name]

    def _record(self, command) -> None:
        self.commands.append((current_thread().name, tuple(command)))

    def _drop_name(self, path: Path) -> None:
        identity = int(path.read_text())
        path.unlink()
        if identity not in {self.namespace(name) for name in self.names()}:
            for link in [link for link, peer in self.links.items() if peer == identity]:
                del self.links[link]
                self.configured.discard(link)

    def run(self, command) -> None:
        self._record(command)
        if command[:3] == ("ip", "netns", "add"):
            name = command[3]
            if name.startswith("ucloud-pool-") and self.block_pool_add is not None:
                if not self.block_pool_add.wait(10):
                    raise TimeoutError("test never released the pool refill")
            with self._guard:
                if name == self.fail_namespace:
                    raise DirectNetworkError("injected netns failure")
                self.root.mkdir(parents=True, exist_ok=True)
                with (self.root / name).open("x") as handle:
                    handle.write(str(next(self._ids)))
        elif command[:3] == ("ip", "link", "add"):
            host, namespace = command[3], command[-1]
            with self._guard:
                if host in self.links:
                    raise DirectNetworkError(f"{host} exists")
                identity = self.namespace(namespace)
                if identity is None:
                    raise DirectNetworkError(f"no namespace {namespace}")
                self.links[host] = identity
        else:
            raise AssertionError(f"unexpected command {command}")

    def batch(self, argv, commands: str) -> None:
        self._record(argv)
        with self._guard:
            if argv[:2] == ("ip", "-n"):
                identity = self.namespace(argv[2])
                if identity is None or identity not in self.links.values():
                    raise DirectNetworkError("guest eth0 is absent")
            else:
                host = commands.split()[3]
                if host not in self.links:
                    raise DirectNetworkError("host veth is absent")
                self.configured.add(host)

    def command_ok(self, command) -> bool:
        self._record(command)
        with self._guard:
            if command[:3] == ("ip", "link", "show"):
                return command[-1] in self.links
            if command[:2] == ("ip", "-n"):
                identity = self.namespace(command[2])
                return identity is not None and identity in self.links.values()
        raise AssertionError(f"unexpected check {command}")

    def best_effort(self, command) -> None:
        self._record(command)
        with self._guard:
            if command[:3] == ("ip", "link", "delete"):
                self.links.pop(command[3], None)
                self.configured.discard(command[3])
            elif command[:3] == ("ip", "netns", "delete"):
                path = self.root / command[3]
                if path.exists():
                    self._drop_name(path)
            else:
                raise AssertionError(f"unexpected cleanup {command}")

    def attach(self, source: Path, target: Path) -> None:
        with self._guard:
            os.link(source, target)  # One inode, as two binds name one nsfs inode.

    def detach(self, path: Path) -> None:
        with self._guard:
            if path.exists():
                self._drop_name(path)


class Crash(Exception):
    pass


def wait_for(predicate, timeout: float = 10.0) -> None:
    deadline = monotonic() + timeout
    while not predicate():
        if monotonic() > deadline:
            raise AssertionError("condition was not reached")
        sleep(0.005)


class NetworkPoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = TemporaryDirectory()
        self.root = Path(self._directory.name).resolve()
        self.kernel = FakeKernel(self.root / "netns")
        self.managers: list[DirectNetworkManager] = []

    def tearDown(self) -> None:
        for manager in self.managers:
            manager.stop_pool()
        self._directory.cleanup()

    def manager(self, pool_size: int) -> DirectNetworkManager:
        manager = self.kernel.install(DirectNetworkManager(
            self.root / "network-slots.json",
            namespace_root=self.root / "netns",
            pool_size=pool_size,
        ))
        self.managers.append(manager)
        return manager

    def state(self) -> dict:
        return json.loads((self.root / "network-slots.json").read_text())

    def write_state(self, **state) -> None:
        (self.root / "network-slots.json").write_text(json.dumps({"version": 1, **state}))

    def assert_pair_owned_by(self, manager, lease) -> None:
        self.assertEqual(
            self.kernel.links[lease.host_interface], self.kernel.namespace(lease.namespace)
        )
        self.assertIn(lease.host_interface, self.kernel.configured)
        self.assertFalse((self.root / "netns" / f"ucloud-pool-{lease.slot}").exists())

    def test_refill_fills_pool_and_ensure_hands_out_without_ip(self) -> None:
        manager = self.manager(3)
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 3)
        self.assertEqual(self.state()["pool"], [1, 2, 3])
        stores: list[str] = []
        store = manager._store

        def counted_store(state, **kwargs):
            stores.append(current_thread().name)
            store(state, **kwargs)

        manager._store = counted_store

        lease = manager.ensure("sandbox-a", 1)

        # One durable write moved slot 1 to the lease; no ip ran for it.
        self.assertEqual(lease.slot, 1)
        self.assertEqual(stores.count(current_thread().name), 1)
        self.assertEqual(self.kernel.ip_by(current_thread().name), [])
        self.assert_pair_owned_by(manager, lease)
        state = self.state()
        self.assertEqual(state["leases"], {"sandbox-a\u00001": 1})
        self.assertNotIn(1, state["pool"])
        wait_for(lambda: len(manager._pool_ready) == 3)
        self.assertEqual(self.state()["pool"], [2, 3, 4])
        self.assertEqual(manager.ensure("sandbox-a", 1), lease)

    def test_exhausted_pool_falls_back_to_synchronous_setup(self) -> None:
        manager = self.manager(2)
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 2)
        self.kernel.block_pool_add = Event()
        first = manager.ensure("a", 1)
        second = manager.ensure("b", 1)
        wait_for(lambda: len(self.state()["pool"]) == 1)  # slot 3 reserved, unfilled

        third = manager.ensure("c", 1)

        self.assertEqual((first.slot, second.slot), (1, 2))
        # A reserved but unfilled pool slot is never leased.
        self.assertEqual(third.slot, 4)
        self.assertIn(
            ("ip", "netns", "add", third.namespace), self.kernel.ip_by(current_thread().name)
        )
        for lease in (first, second, third):
            self.assert_pair_owned_by(manager, lease)
        self.kernel.block_pool_add.set()
        wait_for(lambda: len(manager._pool_ready) == 2)
        self.assertEqual(self.state()["pool"], [3, 5])

    def test_restart_rechecks_partial_pool_and_finishes_interrupted_handoffs(self) -> None:
        setup = self.manager(0)
        complete, partial = setup._pool_lease(1), setup._pool_lease(2)
        setup._ensure_kernel_lease(complete)
        self.kernel.run(("ip", "netns", "add", partial.namespace))  # crashed before its veth
        # Crash after the durable hand-off of slot 3 to "x", before attach;
        # and after slot 4's attach to "y", before the pooled name was dropped.
        handed, attached = setup._pool_lease(3), setup._pool_lease(4)
        setup._ensure_kernel_lease(handed)
        setup._ensure_kernel_lease(attached)
        y = setup._lease("y", 1, 4)
        self.kernel.attach(attached.namespace_path, y.namespace_path)
        handed_namespace = self.kernel.namespace(handed.namespace)
        self.write_state(leases={"x\u00001": 3, "y\u00001": 4}, pool=[1, 2])

        manager = self.manager(3)
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 3)
        before = len(self.kernel.ip_by(current_thread().name))
        x = manager.ensure("x", 1)
        y = manager.ensure("y", 1)

        self.assertEqual(self.state()["pool"], [1, 2, 5])
        for slot in (1, 2, 5):
            pooled = manager._pool_lease(slot)
            self.assertEqual(
                self.kernel.links[pooled.host_interface], self.kernel.namespace(pooled.namespace)
            )
        self.assertEqual((x.slot, y.slot), (3, 4))
        self.assertEqual(self.kernel.namespace(x.namespace), handed_namespace)
        for lease in (x, y):
            self.assert_pair_owned_by(manager, lease)
        commands = self.kernel.ip_by(current_thread().name)[before:]
        self.assertNotIn(("ip", "netns", "add"), [command[:3] for command in commands])

    def test_pool_only_writes_skip_the_directory_fsync_and_survive_losing_it(self) -> None:
        manager = self.manager(2)
        synced: list[bool] = []  # True for a directory
        fsync = os.fsync

        def recorded(descriptor):
            synced.append(stat.S_ISDIR(os.fstat(descriptor).st_mode))
            fsync(descriptor)

        with patch("ucloud_sandboxes.direct_network.os.fsync", recorded):
            lease = manager.ensure("a", 1)
            self.assertEqual(synced, [False, True])
            durable = (self.root / "network-slots.json").read_bytes()
            del synced[:]
            manager.start_pool()
            wait_for(lambda: len(manager._pool_ready) == 2)
            manager.stop_pool()
            self.assertEqual(synced, [False, False])  # Two pool-only writes.
        # OS crash before any later directory sync: the name reverts to the
        # synced write and every kernel pair is gone.
        (self.root / "network-slots.json").write_bytes(durable)
        shutil.rmtree(self.root / "netns")
        self.kernel.links.clear()
        self.kernel.configured.clear()
        restarted = self.manager(2)
        self.assertEqual(restarted.lease("a", 1), lease)
        self.assertEqual(self.state()["leases"], json.loads(durable)["leases"])
        restarted.start_pool()
        wait_for(lambda: len(restarted._pool_ready) == 2)
        self.assertEqual(self.state()["pool"], [2, 3])
        self.assertEqual(restarted.ensure("a", 1), lease)
        self.assert_pair_owned_by(restarted, lease)

    def test_release_of_interrupted_handoff_drops_the_pooled_name(self) -> None:
        manager = self.manager(1)
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 1)
        with patch.object(manager, "_adopt_pooled", side_effect=Crash), self.assertRaises(Crash):
            manager.ensure("a", 1)  # crashes after the durable hand-off
        self.assertEqual(self.state()["leases"], {"a\u00001": 1})
        self.assertTrue((self.root / "netns" / "ucloud-pool-1").exists())
        wait_for(lambda: len(manager._pool_ready) == 1)

        manager.release("a", 1)

        self.assertEqual(self.kernel.names(), {"ucloud-pool-2"})
        self.assertEqual(set(self.kernel.links), {"us2h"})
        self.assertEqual(self.state()["leases"], {})

    def test_unadoptable_handoff_deletes_the_pooled_link_before_its_name(self) -> None:
        # A stale name at the lease's path, or a failed bind. Dropping the
        # pooled name first would free its veth asynchronously.
        manager = self.manager(2)
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 2)
        detached: list[tuple[str, bool]] = []
        detach = manager._detach_namespace
        manager._detach_namespace = lambda path: (detached.append(
            (path.name, f"us{path.name.rsplit('-', 1)[1]}h" in self.kernel.links)), detach(path))
        self.kernel.run(("ip", "netns", "add", manager._lease("stale", 1, 1).namespace))
        stale = manager.ensure("stale", 1)
        with patch.object(manager, "_attach_namespace", side_effect=OSError("bind refused")), \
                self.assertLogs("ucloud_sandboxes.direct_network", "WARNING"):
            refused = manager.ensure("refused", 1)

        self.assertEqual((stale.slot, refused.slot), (1, 2))
        self.assertEqual(detached, [("ucloud-pool-1", False), ("ucloud-pool-2", False)])
        for lease in (stale, refused):
            self.assert_pair_owned_by(manager, lease)

    def test_concurrent_creates_never_share_a_slot_or_pair(self) -> None:
        manager = self.manager(4)
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 4)
        with ThreadPoolExecutor(max_workers=16) as pool:
            leases = list(pool.map(lambda n: manager.ensure(f"s{n}", 1), range(16)))

        self.assertEqual(len({lease.slot for lease in leases}), 16)
        namespaces = [self.kernel.namespace(lease.namespace) for lease in leases]
        self.assertEqual(len(set(namespaces)), 16)
        for lease in leases:
            self.assert_pair_owned_by(manager, lease)
        wait_for(lambda: len(manager._pool_ready) == 4)
        state = self.state()
        self.assertFalse(set(state["pool"]) & set(state["leases"].values()))
        self.assertEqual(len(state["leases"]), 16)

    def test_migration_skips_a_pooled_slot_with_the_source_guest_ip(self) -> None:
        manager = self.manager(2)
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 2)
        lease = manager.ensure("moved", 3, avoid_guest_ips=("100.96.0.3",))
        self.assertEqual(lease.slot, 2)
        self.assert_pair_owned_by(manager, lease)

    def test_a_lease_from_an_older_release_owns_a_pooled_slot(self) -> None:
        self.write_state(leases={"old\u00001": 2}, pool=[1, 2])
        manager = self.manager(2)
        self.assertEqual(manager._load()["pool"], [1])
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 2)
        self.assertEqual(self.state()["pool"], [1, 3])
        self.assertNotIn("us2h", self.kernel.links)
        self.assertEqual(manager.lease("old", 1).slot, 2)

    def test_disabled_pool_is_trimmed_and_its_pairs_deleted(self) -> None:
        setup = self.manager(0)
        for slot in (1, 2):
            setup._ensure_kernel_lease(setup._pool_lease(slot))
        self.write_state(leases={}, pool=[1, 2])
        manager = self.manager(0)
        manager.start_pool()
        wait_for(lambda: self.state()["pool"] == [])
        manager._pool_thread.join(5)
        self.assertFalse(manager._pool_thread.is_alive())
        self.assertEqual(self.kernel.names(), set())
        self.assertEqual(self.kernel.links, {})

    def test_an_agent_that_is_not_the_pool_owner_never_touches_the_pool(self) -> None:
        # --rust-creates: runtime/noded owns state["pool"] and its pairs.
        setup = self.manager(0)
        setup._ensure_kernel_lease(setup._pool_lease(1))
        self.write_state(leases={}, pool=[1])
        manager = self.kernel.install(DirectNetworkManager(
            self.root / "network-slots.json", namespace_root=self.root / "netns",
            pool_size=0, pool_owner=False))
        self.managers.append(manager)
        manager.start_pool()
        self.assertIsNone(manager._pool_thread)
        lease = manager.ensure("a", 1)
        self.assertEqual(lease.slot, 2)
        self.assertEqual(self.state()["pool"], [1])
        self.assertIn("ucloud-pool-1", self.kernel.names())
        self.assertFalse((self.root / "network-slots.json.lock.pool").exists())

    def test_one_process_owns_the_pool(self) -> None:
        owner, other = self.manager(2), self.manager(2)
        owner.start_pool()
        wait_for(lambda: len(owner._pool_ready) == 2)
        with self.assertLogs("ucloud_sandboxes.direct_network", "WARNING"):
            other.start_pool()
            other._pool_thread.join(5)
        self.assertEqual(len(other._pool_ready), 0)
        lease = other.ensure("a", 1)
        self.assertEqual(lease.slot, 3)
        self.assertEqual(owner.ensure("b", 1).slot, 1)

    def test_failed_refill_retries_without_handing_out_the_slot(self) -> None:
        self.kernel.fail_namespace = "ucloud-pool-1"
        manager = self.manager(1)
        attempt = ("ip", "netns", "add", "ucloud-pool-1")
        with patch("ucloud_sandboxes.direct_network._POOL_RETRY_SECONDS", 0.01), \
                self.assertLogs("ucloud_sandboxes.direct_network", "ERROR"):
            manager.start_pool()
            wait_for(lambda: [c for _n, c in self.kernel.commands].count(attempt) >= 2)
            self.assertEqual(len(manager._pool_ready), 0)
            self.assertEqual(manager.ensure("a", 1).slot, 2)
            self.kernel.fail_namespace = None
            wait_for(lambda: len(manager._pool_ready) == 1)
        self.assertEqual(self.state()["pool"], [1])

    def test_refill_waits_for_inflight_ensures(self) -> None:
        manager = self.manager(1)
        entered, release = Event(), Event()

        def slow_kernel_work(_lease):
            entered.set()  # outside the state lock, inside the ensure
            release.wait(5)
            return False

        manager._adopt_pooled = slow_kernel_work
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(manager.ensure, "a", 1)
            self.assertTrue(entered.wait(5))
            manager.start_pool()
            sleep(0.2)
            self.assertEqual(self.kernel.ip_by("ucloud-direct-network-pool"), [])
            release.set()
            pending.result(5)
        wait_for(lambda: len(manager._pool_ready) == 1)


_UNSHARE = ("unshare", "--user", "--map-root-user", "--net", "--mount", "--fork")


def _unprivileged_namespaces() -> bool:
    if sys.platform != "linux" or shutil.which("unshare") is None or shutil.which("ip") is None:
        return False
    try:
        return subprocess.run((*_UNSHARE, "true"), capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@unittest.skipUnless(_unprivileged_namespaces(), "needs unprivileged user namespaces and ip")
class KernelNetworkPoolTests(unittest.TestCase):
    """Real ip, netns, veth and bind-mount hand-off in a disposable user namespace."""

    def test_pool_handoff_with_real_namespaces(self) -> None:
        result = subprocess.run(
            (*_UNSHARE, sys.executable, str(Path(__file__).resolve()), "--kernel-worker"),
            capture_output=True, text=True, timeout=120,
            env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("kernel pool hand-off passed", result.stdout)


def _kernel_worker() -> None:
    def run(*argv):
        subprocess.run(argv, check=True, capture_output=True, timeout=10)

    # It remounts / and /run/netns: refuse the host's initial user namespace.
    assert Path("/proc/self/uid_map").read_text().split() != ["0", "0", "4294967295"]
    run("mount", "--make-rprivate", "/")
    Path("/run/netns").mkdir(parents=True, exist_ok=True)
    run("mount", "-t", "tmpfs", "tmpfs", "/run/netns")
    with TemporaryDirectory() as raw:
        manager = DirectNetworkManager(Path(raw) / "slots.json", pool_size=2)
        manager._ensure_host_rules = lambda: None  # never touch a firewall
        manager.start_pool()
        wait_for(lambda: len(manager._pool_ready) == 2, timeout=30)
        lease = manager.ensure("sandbox-a", 1)
        assert lease.slot == 1, lease
        assert not Path("/run/netns/ucloud-pool-1").exists()
        shown = subprocess.run(
            ("ip", "-n", lease.namespace, "-o", "-4", "address", "show", "dev", "eth0"),
            check=True, capture_output=True, text=True,
        ).stdout
        assert f"{lease.guest_ip}/31" in shown, shown
        host = subprocess.run(
            ("ip", "-o", "-4", "address", "show", "dev", lease.host_interface),
            check=True, capture_output=True, text=True,
        ).stdout
        assert f"{lease.host_ip}/31" in host, host
        subprocess.run(("ip", "netns", "exec", lease.namespace, "ping", "-c1", "-W2", lease.host_ip),
                       check=True, capture_output=True)
        wait_for(lambda: len(manager._pool_ready) == 2, timeout=30)
        manager.release("sandbox-a", 1)
        assert not lease.namespace_path.exists()
        assert not manager._interface_present(lease.host_interface)
        # A crash after the bind, before the pooled name is dropped: both
        # names are one nsfs inode, so the next ensure drops only the pool's.
        def crash(_path):
            raise Crash

        manager._detach_namespace = crash
        try:
            manager.ensure("sandbox-b", 1)
            raise AssertionError("the hand-off did not crash")
        except Crash:
            del manager._detach_namespace
        lease = manager.ensure("sandbox-b", 1)
        assert not manager._pool_lease(lease.slot).namespace_path.exists()
        subprocess.run(("ip", "netns", "exec", lease.namespace, "ping", "-c1", "-W2", lease.host_ip),
                       check=True, capture_output=True)
        manager.stop_pool()
    print("kernel pool hand-off passed")


if __name__ == "__main__" and sys.argv[1:] == ["--kernel-worker"]:
    _kernel_worker()
elif __name__ == "__main__":
    unittest.main()
