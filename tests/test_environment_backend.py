import errno
from pathlib import Path
import socket
import os
import stat
from types import SimpleNamespace
from unittest.mock import patch
from threading import Thread
import time
import unittest

# Import the module, not the TestCase: discovery would rerun it here.
from tests import test_environment_artifact as artifact_fixtures
from ucloud_sandboxes.environment_backend import EnvironmentBackend, EnvironmentBackendClient, EnvironmentBackendServer, mount_has_dependents
from ucloud_sandboxes.environment_nbd import EnvironmentReadWorkers, ReadOnlyEnvironmentDevice, ReadOnlyNbdExport, REQUEST, REPLY, REQUEST_MAGIC, REPLY_MAGIC

TEST_TIER = "contract"


class EnvironmentBackendTests(artifact_fixtures.EnvironmentArtifactTests):
    def test_bind_view_gc_preserves_backing_and_overlay_dependency_fences(self):
        device = self.root.stat().st_dev
        identity = f"{os.major(device)}:{os.minor(device)}"
        peers = f"100 1 {identity} / /component ro - erofs /dev/nbd0 ro\n"
        with patch("ucloud_sandboxes.environment_backend.Path.read_text", return_value=peers):
            # Image-view removal can leave the owned canonical component mount;
            # backing disconnect cannot ignore another retained filesystem bind.
            self.assertTrue(mount_has_dependents(self.root))
            self.assertFalse(mount_has_dependents(self.root, include_bind_mounts=False))
        overlay = f"101 1 0:123 / /sandbox rw - overlay overlay rw,lowerdir={self.root},upperdir=/upper,workdir=/work\n"
        with patch("ucloud_sandboxes.environment_backend.Path.read_text", return_value=peers + overlay):
            self.assertTrue(mount_has_dependents(self.root))
            self.assertTrue(mount_has_dependents(self.root, include_bind_mounts=False))

    def test_device_waits_for_capacity_and_owner_publication_before_mount(self):
        device = self.root / "sysfs"
        device.mkdir()
        (device / "size").write_text("0\n")
        waits = []
        def publish(delay):
            waits.append(delay)
            (device / "size").write_text("8\n")
            (device / "pid").write_text("123\n")
        export = SimpleNamespace(healthy=True, _export=SimpleNamespace(cancel=SimpleNamespace(wait=publish)))
        ReadOnlyEnvironmentDevice._await_ready(export, device, 4096)
        self.assertEqual(waits, [.005])
        (device / "size").write_text("0\n")
        with patch("ucloud_sandboxes.environment_nbd.time.monotonic", side_effect=[0, 6]), \
             self.assertRaisesRegex(RuntimeError, "did not become ready"):
            ReadOnlyEnvironmentDevice._await_ready(export, device, 4096)
        export.healthy = False
        with self.assertRaisesRegex(RuntimeError, "did not become ready"):
            ReadOnlyEnvironmentDevice._await_ready(export, device, 4096)

    def test_devices_are_sized_in_blocks_beyond_two_gib(self):
        from ucloud_sandboxes.environment_nbd import SET_BLKSIZE, SET_SIZE_BLOCKS, size_ioctls
        size = 3 * 1024 ** 3 + 4096
        self.assertEqual(size_ioctls(size), ((SET_BLKSIZE, 4096), (SET_SIZE_BLOCKS, size // 4096)))
        for argument in (value for _, value in size_ioctls(size)):
            self.assertLess(argument, 2 ** 31)
        for invalid in (0, 4097):
            with self.assertRaises(ValueError):
                size_ioctls(invalid)

    def test_device_owner_checks_precede_all_configuration(self):
        device = self.root / "device"
        device.touch()
        block = SimpleNamespace(st_mode=stat.S_IFBLK, st_rdev=os.makedev(43, 0))
        with patch("ucloud_sandboxes.environment_nbd.os.fstat", return_value=block), \
             patch("ucloud_sandboxes.environment_nbd.fcntl.flock", side_effect=BlockingIOError()), \
             patch("ucloud_sandboxes.environment_nbd.fcntl.ioctl") as ioctl:
            with self.assertRaises(OSError) as raised:
                ReadOnlyEnvironmentDevice(device, self.component, None, None, trusted_keys=self.registry.trusted_keys)
            self.assertEqual(raised.exception.errno, errno.EBUSY)
            ioctl.assert_not_called()
        with patch("ucloud_sandboxes.environment_nbd.os.fstat", return_value=block), \
             patch("ucloud_sandboxes.environment_nbd.Path.read_text", return_value="123\n"), \
             patch("ucloud_sandboxes.environment_nbd.fcntl.ioctl") as ioctl:
            with self.assertRaises(OSError) as raised:
                ReadOnlyEnvironmentDevice(device, self.component, None, None, trusted_keys=self.registry.trusted_keys)
            self.assertEqual(raised.exception.errno, errno.EBUSY)
            ioctl.assert_not_called()

    def test_readonly_protocol_and_disconnect(self):
        cache = self.cache()
        workers = EnvironmentReadWorkers(concurrency=2)
        self.addCleanup(workers.close)
        client, server = socket.socketpair()
        client.settimeout(3)
        self.addCleanup(client.close)
        export = ReadOnlyNbdExport(server, self.component, cache, workers)
        self.addCleanup(export.close)
        handle = b"12345678"
        def receive(count):
            data = b""
            while len(data) < count:
                data += client.recv(count - len(data))
            return data
        client.sendall(REQUEST.pack(REQUEST_MAGIC, 0, handle, 12, 11))
        self.assertEqual(REPLY.unpack(receive(REPLY.size)), (REPLY_MAGIC, 0, handle))
        self.assertEqual(receive(11), self.bytes[12:23])
        client.sendall(REQUEST.pack(REQUEST_MAGIC, 1, handle, 0, 4) + b"EVIL")
        self.assertEqual(REPLY.unpack(receive(REPLY.size)), (REPLY_MAGIC, errno.EROFS, handle))
        client.sendall(REQUEST.pack(REQUEST_MAGIC, 0, handle, len(self.bytes), 1))
        self.assertEqual(REPLY.unpack(receive(REPLY.size)), (REPLY_MAGIC, errno.EINVAL, handle))
        client.sendall(REQUEST.pack(REQUEST_MAGIC, 2, handle, 0, 0))
        self.assertEqual(client.recv(1), b"")
        self.assertEqual(cache.read(self.component, 0, 4), b"aaaa")

    def test_frontend_restart_keeps_backend_and_busy_mount(self):
        mounts, devices = set(), []
        busy = set()
        def mount(device, target):
            mounts.add(target)
        def unmount(target):
            if target in busy:
                raise OSError(errno.EBUSY, "live overlay")
            mounts.remove(target)
        class Device:
            def __init__(self, path, *args, **kwargs):
                self.path, self.closed = path, False
                devices.append(self)
            def close(self):
                self.closed = True
        backend = EnvironmentBackend(self.root / "backend", self.registry, devices=[Path("/dev/nbd-test")],
            device_factory=Device, mount=mount, unmount=unmount, mounted=lambda path: path in mounts, referenced=lambda _: False)
        self.addCleanup(backend.close)
        endpoint = self.root / "backend.sock"
        server = EnvironmentBackendServer(endpoint, backend)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), thread.join(), server.server_close()))
        first = EnvironmentBackendClient(endpoint)
        mounted = first.ensure(self.digest)
        del first
        replacement = EnvironmentBackendClient(endpoint)
        self.assertEqual(replacement.ensure(self.digest), mounted)
        self.assertEqual(len(devices), 1)
        busy.add(mounted)
        self.assertFalse(replacement.drop(self.digest))
        self.assertFalse(devices[0].closed)
        with self.assertRaisesRegex(RuntimeError, "live filesystem"):
            backend.close()
        busy.clear()
        self.assertTrue(replacement.drop(self.digest))
        self.assertTrue(devices[0].closed)

    def test_a_device_cache_over_budget_detaches_the_least_recently_used_idle_components(self):
        # nydusd's shared filecache has no eviction of its own: cache_bytes bounds it.
        from tests.test_environment_artifact import sign_component
        digests = [self.digest]
        for index in (1, 2):
            image = self.root / f"image-{index}.erofs"
            image.write_bytes(bytes([index]) * 8192)
            digests.append(self.registry.publish(
                image, sign_component(image, source_image="sha256:" + str(index) * 64, signing_key=self.key),
                tag=f"fixture-{index}"))
        mounts, busy, closed, held, MIB = set(), set(), [], {"bytes": 0}, 1024 ** 2

        class Device:
            def __init__(self, path, component, *args, **kwargs):
                self.path, self.component = path, component

            def close(self):
                closed.append(self.path)
                held["bytes"] -= MIB

        class Factory:
            def __call__(self, path, component, *args, **kwargs):
                held["bytes"] += MIB
                return Device(path, component)

            def cache_bytes(self):
                return held["bytes"]
        backend = EnvironmentBackend(self.root / "budget", self.registry, cache_bytes=5 * MIB // 2,
                                     devices=[Path(f"/dev/nbd-test{n}") for n in range(4)], device_factory=Factory(),
                                     mount=lambda device, target: mounts.add(target), unmount=mounts.discard,
                                     mounted=lambda path: path in mounts, referenced=lambda path: path in busy)
        self.addCleanup(backend.close)
        first = Path(backend.ensure(digests[0]))
        backend.ensure(digests[1])
        busy.add(first)  # A sandbox's overlay holds the oldest one.
        backend.ensure(digests[2])  # 3 MiB > 2.5 MiB: the oldest idle one goes.
        self.assertIn(first, mounts)
        self.assertEqual(sorted(backend._active), sorted([digests[0], digests[2]]))
        self.assertEqual(held["bytes"], 2 * MIB)
        self.assertEqual(backend.pressure(), {"active_components": 2, "over_cache_budget": False})
        held["bytes"] += MIB  # Idle images the agent keeps mounted hold it over: only the agent's sweep frees it.
        self.assertEqual(backend.pressure(), {"active_components": 2, "over_cache_budget": True})
        held["bytes"] -= MIB
        busy.clear()
        self.assertTrue(backend.ensure(digests[1]))  # Detached images attach again.

    def test_a_create_burst_is_queued_not_refused_with_eagain(self):
        # The server is not accepting yet: every call must wait in its backlog.
        endpoint = self.root / "burst.sock"
        server = EnvironmentBackendServer(endpoint, SimpleNamespace(metrics=lambda: {"ok": 1},
                                                                    pressure=lambda: {"pressure": 1}))
        self.addCleanup(server.server_close)
        results = []
        calls = [Thread(target=lambda: results.append(EnvironmentBackendClient(endpoint)._call({"method": "metrics"}, 10)))
                 for _ in range(64)]
        for call in calls:
            call.start()
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), thread.join()))
        for call in calls:
            call.join(10)
        self.assertEqual(results, [{"ok": 1}] * 64)
        self.assertEqual(EnvironmentBackendClient(endpoint).pressure(), {"pressure": 1})

    def test_client_retries_a_full_accept_queue(self):
        endpoint = str(self.root / "full.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(listener.close)
        listener.bind(endpoint)
        listener.listen(0)
        listener.settimeout(5)
        fillers = []
        while True:  # Fill the accept queue until a connect is refused.
            filler = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.addCleanup(filler.close)
            filler.setblocking(False)
            try:
                filler.connect(endpoint)
            except BlockingIOError:
                break
            fillers.append(filler)
        result = []
        call = Thread(target=lambda: result.append(EnvironmentBackendClient(endpoint)._call({"method": "metrics"}, 5)))
        call.start()
        time.sleep(0.2)  # The client is now retrying EAGAIN.
        for _ in fillers:  # Accept is FIFO: the queued fillers come first.
            listener.accept()[0].close()
        connection = listener.accept()[0]
        self.assertTrue(connection.makefile("rb").readline().endswith(b"\n"))
        connection.sendall(b'{"result":{"ok":1}}\n')
        connection.close()
        call.join(5)
        self.assertEqual(result, [{"ok": 1}])

    def test_ambiguous_mount_and_cleanup_failure_retain_io_owner(self):
        mounts, devices = set(), []
        fail_unmount = True
        def mount(device, target):
            mounts.add(target)
            raise RuntimeError("mount acknowledgment lost")
        def unmount(target):
            if fail_unmount:
                raise OSError(errno.EBUSY, "still mounted")
            mounts.remove(target)
        class Device:
            healthy = True
            def __init__(self, path, *args, **kwargs):
                self.path, self.closed, self.fail_close = path, False, True
                devices.append(self)
            def close(self):
                if self.fail_close:
                    self.fail_close = False
                    raise RuntimeError("close acknowledgment lost")
                self.closed = True
        backend = EnvironmentBackend(self.root / "backend", self.registry, devices=[Path("/dev/nbd-test")],
            device_factory=Device, mount=mount, unmount=unmount, mounted=lambda path: path in mounts,
            referenced=lambda _: False)
        self.addCleanup(backend.close)
        with self.assertRaisesRegex(RuntimeError, "mount acknowledgment"):
            backend.ensure(self.digest)
        self.assertFalse(devices[0].closed)
        self.assertIn(self.digest, backend._active)
        self.assertEqual(len(mounts), 1)
        fail_unmount = False
        with self.assertRaisesRegex(RuntimeError, "close acknowledgment"):
            backend.drop(self.digest)
        self.assertEqual(mounts, set())
        self.assertTrue(backend.drop(self.digest))
        self.assertTrue(devices[0].closed)

    def test_restart_fences_lost_mount_and_untrusted_before_privilege(self):
        root = self.root / "backend"
        root.mkdir(mode=0o700)
        (root / "components").mkdir(mode=0o700)
        (root / "components" / self.digest[7:]).mkdir(parents=True, mode=0o700)
        with self.assertRaisesRegex(RuntimeError, "lost with retained mounts"):
            EnvironmentBackend(root, self.registry, mounted=lambda path: True)
        backend = EnvironmentBackend(root, self.registry, devices=[Path("/dev/nbd-test")],
            device_factory=lambda *a, **kw: self.fail("untrusted content reached block-device allocation"),
            mounted=lambda path: False)
        self.addCleanup(backend.close)
        backend.registry.trusted_keys.clear()
        with self.assertRaisesRegex(ValueError, "not trusted"):
            backend.ensure(self.digest)


if __name__ == "__main__":
    unittest.main()
