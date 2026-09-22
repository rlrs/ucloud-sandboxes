"""Cross-version sealed-layer migration on a disposable Linux ublk worker."""
from __future__ import annotations

import argparse
import contextlib
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import ThreadingHTTPServer
import json
import multiprocessing
import os
from pathlib import Path
import socket
import subprocess
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from runtime.storage_native.qualify_volume import GIB, Qualifier  # noqa: E402
from runtime.storage_native.range_blob_server import RangeBlobHandler  # noqa: E402
from ucloud_sandboxes.storage_native import StorageNativeError  # noqa: E402


def serve_blobs(root: str, port_pipe):
    server = ThreadingHTTPServer(("127.0.0.1", 0), RangeBlobHandler)
    server.root = Path(root) / "blobs"
    server.metrics = Path(root) / "remote-reads.json"
    server.requests = server.bytes_served = 0
    port_pipe.send(server.server_port)
    port_pipe.close()
    server.serve_forever()


class MigrationQualifier(Qualifier):
    @contextlib.contextmanager
    def _phase(self, name, device=None):
        pid = self.daemon.pid if self.daemon is not None else None
        with super()._phase(name, device):
            yield
        if self.daemon is not None and self.daemon.pid != pid:
            # /proc counters belong to a process, not to a phase spanning two
            # daemons. Subtracting across migration gives bogus negatives.
            self.phases[name]["counters"] = None
            self.phases[name]["counter_note"] = "daemon changed during phase"

    def __init__(self, *, destination: Path, reject_legacy: bool, full_cache: bool, **kwargs):
        super().__init__(**kwargs)
        self.destination = destination
        self.reject_legacy = reject_legacy
        self.full_cache = full_cache
        self.blob_process = None
        self.blob_port = None
        self.remote_layer = None
        self.cache_mount = None

    def _freeze_and_seal(self, device, mount_path):
        layer = super()._freeze_and_seal(device, mount_path)
        # Exercise our custom streaming export before crossing versions. Only
        # the immutable blob is transferred; writable uppers never cross.
        root = self.test_root
        blobs = root / "blobs"
        blobs.mkdir()
        target = blobs / "export"
        listener = socket.socket(socket.AF_UNIX)
        endpoint = root / "export.sock"
        listener.bind(str(endpoint))
        listener.listen(1)
        listener.settimeout(30)

        def receive():
            connection, _ = listener.accept()
            with connection, target.open("wb") as out:
                connection.settimeout(60)
                while chunk := connection.recv(1024 * 1024):
                    out.write(chunk)

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                transfer = executor.submit(receive)
                descriptor = self.client.export_dense_layer(
                    source_layer_path=root / "layers" / "generation-1.commit",
                    stream_socket_path=endpoint,
                )
                transfer.result(timeout=90)
        finally:
            listener.close()
            endpoint.unlink(missing_ok=True)
        digest = "sha256:" + hashlib.sha256(target.read_bytes()).hexdigest()
        assert descriptor.digest == digest and descriptor.size == target.stat().st_size
        target.rename(blobs / digest)
        self.remote_layer = {"digest": digest, "size": descriptor.size}
        # mmap verification can fault while holding Python's GIL. Serving the
        # fault's remote reads from another thread in this process deadlocks.
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        self.blob_process = context.Process(target=serve_blobs, args=(str(root), sender))
        self.blob_process.start()
        sender.close()
        try:
            if not receiver.poll(15):
                raise TimeoutError("range server failed to bind")
            self.blob_port = receiver.recv()
        finally:
            receiver.close()
        return layer

    def _create_resumed_device(self, layer):
        assert not self.devices and not self.mounts
        root = self.test_root
        self.client.shutdown()
        self.daemon.wait(timeout=30)
        assert self.daemon.returncode == 0
        (root / "ublk.sock").unlink(missing_ok=True)
        config = json.loads((root / "global.json").read_text())
        config["cacheConfig"]["cacheDir"] = str(root / "destination-cache")
        if self.full_cache:
            # A bounded loop filesystem contains the ENOSPC experiment. Never
            # fill the VM's root disk or an existing cache filesystem.
            image = root / "cache-disk.img"
            with image.open("wb") as f:
                f.truncate(64 * 1024**2)
            self._command("mkfs.ext4", "-q", "-F", "-m", "0", str(image))
            self.cache_mount = root / "limited-cache"
            self.cache_mount.mkdir()
            self._command("mount", "-o", "loop", str(image), str(self.cache_mount))
            config["cacheConfig"]["cacheDir"] = str(self.cache_mount / "blocks")
            (self.cache_mount / "blocks").mkdir()
            stat = os.statvfs(self.cache_mount)
            available = stat.f_bavail * stat.f_frsize
            with (self.cache_mount / "filler").open("wb") as f:
                os.posix_fallocate(f.fileno(), 0, available - 64 * 1024)
            self.result["cache_free_bytes_before_restore"] = (
                os.statvfs(self.cache_mount).f_bavail * stat.f_frsize
            )
        (root / "global.json").write_text(json.dumps(config))
        command = list(self.daemon.args)
        command[0] = str(self.destination)
        command.extend([
            "--enable-pool", "--pool-low-watermark", "0",
            "--pool-high-watermark", "4", "--pool-startup-prewarm", "false",
        ])
        with (root / "destination-daemon.log").open("w") as log:
            self.daemon = subprocess.Popen(command, stdout=log, stderr=log)
        self.client.wait_ready(timeout_seconds=30)
        self.result["destination_backend_sha256"] = hashlib.sha256(self.destination.read_bytes()).hexdigest()
        if self.reject_legacy:
            upper_paths = [root / "runtime-initial" / name for name in ("upper.data", "upper.index")]
            before = [hashlib.sha256(p.read_bytes()).hexdigest() for p in upper_paths]
            try:
                response = self.client._call({
                    "kind": "acquire_overlaybd",
                    "image_config": str(root / "runtime-initial" / "image.json"),
                    "global_config": str(root / "global.json"),
                    "virtual_size": self.virtual_size,
                    "access_mode": "exclusive",
                })
            except StorageNativeError as exc:
                assert "unsupported hybrid writable upper format" in str(exc), str(exc)
            else:
                self.client.delete(int(response["dev_id"]))
                raise AssertionError("candidate accepted a legacy writable upper")
            assert before == [hashlib.sha256(p.read_bytes()).hexdigest() for p in upper_paths]
            self.result["legacy_upper_rejected_without_modification"] = True
        source = root / "source-remote.json"
        source.write_text(json.dumps({
            "repoBlobUrl": f"http://127.0.0.1:{self.blob_port}/v2/snapshots/blobs",
            "lowers": [self.remote_layer], "upper": {}, "resultFile": "",
        }))
        device = self.client.create_runtime_device(
            source_image_config=source, global_config=root / "global.json",
            runtime_dir=root / "runtime-resumed", virtual_size=self.virtual_size,
            owner_id="qualification-resumed", upper_mode="hybridLogStructured",
        )
        self._register_device(device)
        return device

    def _cleanup(self):
        try:
            super()._cleanup()
        finally:
            # runsc keeps this namespace bind mount after deleting the last
            # sandbox. It belongs only to this qualifier's private state root.
            if self.runsc is not None and self.test_root is not None:
                null_netns = self.test_root / "gvisor-runsc" / "null-netns"
                if os.path.ismount(null_netns):
                    self._command("umount", str(null_netns))
            if self.blob_process is not None:
                self.blob_process.terminate()
                self.blob_process.join(timeout=5)
                stats = self.test_root / "remote-reads.json"
                if stats.exists():
                    self.result["remote_bytes_served"] = json.loads(stats.read_text())["bytes_served"]
            if self.cache_mount is not None:
                self._command("umount", str(self.cache_mount))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-daemon", required=True, type=Path)
    parser.add_argument("--new-daemon", required=True, type=Path)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--runsc", type=Path)
    parser.add_argument("--conformance-workload", type=Path)
    parser.add_argument("--noop-workload", type=Path)
    args = parser.parse_args()
    result = {"status": "failed", "cases": {}}
    try:
        for name, origin, destination, reject, full in (
            ("upgrade", args.old_daemon, args.new_daemon, True, False),
            ("rollback_via_sealed_layer", args.new_daemon, args.old_daemon, False, False),
            ("upgrade_with_full_cache", args.old_daemon, args.new_daemon, True, True),
        ):
            qualifier = MigrationQualifier(
                daemon_binary=origin.resolve(), destination=destination.resolve(),
                reject_legacy=reject, full_cache=full,
                work_root=args.work_root.resolve(), output=args.output.with_name(name + ".json").resolve(),
                virtual_size=GIB, upper_mode="hybridLogStructured",
                runsc=args.runsc.resolve() if args.runsc else None,
                conformance_workload=args.conformance_workload.resolve() if args.conformance_workload else None,
                noop_workload=args.noop_workload.resolve() if args.noop_workload else None,
            )
            result["cases"][name] = qualifier.result
            qualifier.run()
        result["status"] = "passed"
    finally:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
