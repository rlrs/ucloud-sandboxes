#!/usr/bin/env python3
"""Isolated Linux EROFS/NBD qualification; never targets production state.

Exercises real OCI HTTP reads, signature/chunk verification, composition,
OverlayRootfsManager and a live gVisor guest across frontend process replacement.
The in-process registry is a fixture: this does not claim WAN latency results.

``--prefetch`` publishes signed metadata hints (C2.2), runs the backend with
its metrics exported, and then attaches again on a fresh chunk cache that
keeps only the first run's startup traces (C2.3), with a second live guest.
"""

import argparse
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
from threading import Thread
import time
from urllib.parse import unquote
from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from ucloud_sandboxes.environment_artifact import (
    EnvironmentArtifactRegistry,
    OCI_IMAGE,
    attach_environment_to_image,
    canonical_bytes,
    content_digest,
    publish_environment,
    sign_component,
)
from ucloud_sandboxes.environment_backend import (
    EnvironmentBackend,
    EnvironmentBackendClient,
    EnvironmentBackendServer,
    PrefetchPolicy,
)
from ucloud_sandboxes.environment_builder import allowlisted_build_view
from ucloud_sandboxes.environment_config import configured_environment_registry
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.environment_metadata import sign_metadata_hint
from ucloud_sandboxes.environment_rootfs import EnvironmentRootfsStore
from ucloud_sandboxes.image_rootfs import OverlayRootfsManager
from ucloud_sandboxes.managed_registry import RegistryRequestError


class FixtureRegistry:
    def __init__(self):
        self.blobs, self.manifests, self.uploads = {}, {}, {}
        self.downloaded_bytes = 0
        self.layer_sizes = {}

    def upload_blob_file(self, repository, path, digest, size):
        payload = Path(path).read_bytes()
        if len(payload) != size or content_digest(payload) != digest:
            raise ValueError("fixture upload digest/size mismatch")
        self.blobs[digest] = payload
        return digest

    def manifest_layers(self, repository, reference):
        return SimpleNamespace(layers=[SimpleNamespace(size=size) for size in self.layer_sizes[reference]])

    def blob_exists(self, repository, digest):
        return digest in self.blobs

    def start_blob_upload(self, repository):
        token = str(len(self.uploads))
        self.uploads[token] = b""
        return token

    def upload_blob_chunk(self, location, chunk):
        self.uploads[location] += chunk
        return location

    def finish_blob_upload(self, location, digest):
        self.blobs[digest] = self.uploads.pop(location)

    def abort_blob_upload(self, location):
        self.uploads.pop(location, None)

    def put_manifest(self, repository, tag, payload, *, media_type):
        self.manifests[content_digest(payload)] = self.manifests[tag] = payload

    def manifest_document(self, repository, digest):
        try:
            return json.loads(self.manifests[digest]), {}
        except KeyError:
            raise RegistryRequestError(404, "GET", digest, "MANIFEST_UNKNOWN") from None

    def blob_bytes(self, repository, digest, *, max_bytes):
        return self.blobs[digest][: max_bytes + 1]


def frontend(path):
    settings = json.loads(path.read_bytes())
    root = Path(settings["root"])
    registry = configured_environment_registry(
        settings["url"], "environments", root / "keys.json"
    )
    store = EnvironmentRootfsStore(
        root / "images", registry, EnvironmentBackendClient(root / "backend.sock")
    )
    if "image_id" in settings:
        with store.mounted_rootfs_lease(
            settings["image_id"], rootfs_identity_sha256=settings["fingerprint"]
        ) as mounted:
            print(json.dumps({"rootfs": str(mounted)}))
        return
    manager = OverlayRootfsManager(
        store, writable_root=root / "writable", bundle_root=root / "bundles"
    )
    from ucloud_sandboxes.environment_rootfs import EnvironmentImageRuntime
    from ucloud_sandboxes.images import ImageManager, ImageStore
    image_api = ImageManager(ImageStore(root / "images.sqlite"), EnvironmentImageRuntime(store))
    record, result = image_api.pull(settings["image_ref"], image_id="environment-probe")
    if result.exit_code or not record.manifest_digest:
        raise RuntimeError("canonical image API did not resolve the immutable environment")
    with store.operation_lease(settings["image_ref"]) as image:
        lease = manager.prepare(
            sandbox_id="environment-probe",
            sandbox_generation=1,
            image=image,
            config_template=settings["config"],
        )
        print(
            json.dumps(
                {
                    "image_id": image.image_id,
                    "fingerprint": image.rootfs_identity_sha256,
                    "container_id": lease.sandbox.container_id,
                    "bundle": str(lease.sandbox.bundle),
                }
            )
        )


PREFETCH_METRICS = (
    "metadata_hint_present", "metadata_prefetch_chunks", "metadata_prefetch_bytes",
    "metadata_prefetch_seconds", "metadata_prefetch_wait_timeouts", "trace_hint_present",
    "trace_hint_absent", "trace_prefetch_chunks", "trace_prefetch_bytes", "trace_prefetch_seconds",
    "trace_recordings_started", "traces_recorded", "trace_chunks_recorded", "prefetch_joined_reads",
    "prefetch_start_failures", "misses", "downloaded_bytes",
)


def metered_backend(path):
    """The artifact backend as ``serve_backend`` builds it, exporting metrics.

    Only the trace window is shortened, so that a qualification run closes it.
    The process is killed, never closed, exactly as the plain backend is.
    """
    settings = json.loads(path.read_bytes())
    registry = configured_environment_registry(settings["url"], "environments", Path(settings["keys"]))
    backend = EnvironmentBackend(Path(settings["root"]), registry,
                                 prefetch=PrefetchPolicy(trace_window_seconds=settings["trace_window_seconds"]))
    server = EnvironmentBackendServer(settings["socket"], backend)
    Thread(target=server.serve_forever, daemon=True).start()
    metrics = Path(settings["metrics"])
    while True:
        metrics.with_suffix(".tmp").write_text(json.dumps(backend.metrics()))
        metrics.with_suffix(".tmp").replace(metrics)
        time.sleep(0.05)


def prefetch_metrics(path):
    time.sleep(0.1)  # Two export periods, so the snapshot postdates the caller's last event.
    values = json.loads(path.read_text())
    return {name: values[name] for name in PREFETCH_METRICS}


def wait_for(predicate, seconds, what):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() > deadline:
            raise RuntimeError(what + " did not happen in time")
        time.sleep(0.05)


def start_backend(command, log_path, socket_path):
    log = log_path.open("w")
    backend = subprocess.Popen(command, stdout=log, stderr=log)
    deadline = time.monotonic() + 10
    while not socket_path.exists():
        if backend.poll() is not None or time.monotonic() > deadline:
            raise RuntimeError("backend failed to start: " + log_path.read_text())
        time.sleep(0.02)
    return backend


def start_guest(runsc, bundle, identifier, directory):
    """Run the fixture guest until it reports GUEST_READY; returns (process, seconds)."""
    started = time.monotonic()
    guest = subprocess.Popen(
        runsc + ["run", "--bundle=" + str(bundle), identifier],
        stdout=(directory / "guest.stdout").open("w"),
        stderr=(directory / "guest.stderr").open("w"),
        text=True,
    )
    deadline = time.monotonic() + 15
    while "GUEST_READY" not in (directory / "guest.stdout").read_text():
        if guest.poll() is not None or time.monotonic() > deadline:
            raise RuntimeError(
                "guest failed to become live: " + (directory / "guest.stderr").read_text()
            )
        time.sleep(0.05)
    return guest, time.monotonic() - started


def finish_guest(guest, directory):
    guest.wait(timeout=10)
    stdout, stderr = (directory / "guest.stdout").read_text(), (directory / "guest.stderr").read_text()
    assert guest.returncode == 0 and "GUEST_OK" in stdout, stderr


def unmount_under(root):
    """Unmount only mounts under this freshly allocated qualification directory."""
    mounts = [
        line.split()[4].replace("\\040", " ")
        for line in Path("/proc/self/mountinfo").read_text().splitlines()
        if line.split()[4].startswith(str(root) + "/")
    ]
    errors = []
    for mount in sorted(mounts, key=lambda value: (value.count("/"), value), reverse=True):
        completed = subprocess.run(["umount", mount], capture_output=True, text=True, timeout=20)
        if completed.returncode:
            errors.append(completed.stderr)
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runsc", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--layers", action="store_true", help="Also qualify shared layers against Docker overlay2")
    parser.add_argument("--prefetch", action="store_true",
                        help="Publish metadata hints and qualify startup-trace replay on a fresh cache")
    parser.add_argument("--frontend", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--metered-backend", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.frontend:
        frontend(args.frontend)
        return 0
    if args.metered_backend:
        metered_backend(args.metered_backend)
        return 0
    if os.geteuid() != 0 or not args.runsc or not args.output:
        parser.error("requires root, --runsc and --output on an isolated Linux host")
    root = Path(tempfile.mkdtemp(prefix="ucloud-environment-qualification-"))
    result = {
        "passed": False,
        "scope": "local fixture OCI HTTP, real Linux NBD/EROFS and live gVisor",
        "runsc_sha256": hashlib.sha256(args.runsc.read_bytes()).hexdigest(),
    }
    backend = guest = server = None
    runsc = [
        str(args.runsc),
        "--root=" + str(root / "runsc"),
        "--platform=systrap",
        "--network=none",
        "--debug",
        "--debug-log=" + str(root / "runsc-debug-"),
        "--application-memory-file-dir=" + str(root / "writable"),
    ]
    identifier = None
    try:
        source = root / "source"
        source.mkdir()
        for directory in ("bin", "etc", "tmp", "proc", "dev", "opaque", "unrelated"):
            (source / directory).mkdir()
        shutil.copy2("/usr/bin/busybox", source / "bin/busybox")
        (source / "etc/hello").write_text("immutable\n")
        os.setxattr(source / "etc/hello", "user.fixture", b"preserved")
        os.link(source / "etc/hello", source / "etc/hardlink")
        (source / "etc/symlink").symlink_to("hello")
        (source / "delete-me").write_text("hidden")
        (source / "opaque/old").write_text("hidden")
        for index in range(64):
            (source / "unrelated" / f"payload-{index:02d}").write_bytes(
                hashlib.shake_256(str(index).encode()).digest(1024**2)
            )
        toolkit = root / "toolkit"
        toolkit.mkdir()
        (toolkit / "opaque").mkdir()
        (toolkit / "opaque/new").write_text("visible")
        os.setxattr(toolkit / "opaque", "trusted.overlay.opaque", b"y")
        os.mknod(toolkit / "delete-me", stat.S_IFCHR | 0o600, os.makedev(0, 0))
        key = Ed25519PrivateKey.generate()
        public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        keys = {content_digest(public): public}
        (root / "keys.json").write_text(
            json.dumps(
                {
                    identity: base64.b64encode(value).decode()
                    for identity, value in keys.items()
                }
            )
        )
        fixture = FixtureRegistry()
        registry = EnvironmentArtifactRegistry(fixture, "environments", keys)
        components, total_bytes = [], 0
        for name, tree, allowlist in (
            (
                "base",
                source,
                [
                    "bin",
                    "etc",
                    "tmp",
                    "proc",
                    "dev",
                    "opaque",
                    "unrelated",
                    "delete-me",
                ],
            ),
            ("toolkit", toolkit, ["opaque", "delete-me"]),
        ):
            view, image = root / (name + "-view"), root / (name + ".erofs")
            allowlisted_build_view(tree, view, allowlist)
            subprocess.run(
                ["mkfs.erofs", "-T", "0", str(image), str(view)],
                check=True,
                capture_output=True,
            )
            component = sign_component(
                image, source_image="sha256:" + "1" * 64, signing_key=key
            )
            hint = None
            if args.prefetch:
                hint, walked = sign_metadata_hint(image, component, key)
                result.setdefault("metadata_hints", {})[name] = {
                    "image_bytes": walked.image_bytes, "metadata_bytes": walked.metadata_bytes,
                    "chunks": len(hint.chunks), "chunk_count": hint.chunk_count, "complete": hint.complete}
            components.append(registry.publish(image, component, tag=name, metadata=hint))
            total_bytes += image.stat().st_size
        environment_root = publish_environment(
            registry,
            source_image="sha256:" + "1" * 64,
            environment=EnvironmentManifest(components[0], toolkits=(components[1],)),
            image_config={"Cmd": ["/bin/busybox", "sh"]},
            signing_key=key,
            tag="environment",
        )
        fixture.put_manifest(
            "environments",
            "image",
            canonical_bytes(
                {
                    "schemaVersion": 2,
                    "mediaType": OCI_IMAGE,
                    "config": {"digest": "sha256:" + "1" * 64},
                    "layers": [],
                }
            ),
            media_type=OCI_IMAGE,
        )
        image_digest = attach_environment_to_image(
            registry,
            image_repository="environments",
            image_reference="image",
            environment_digest=environment_root,
        )

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parts = self.path.split("/")
                try:
                    kind, identity = parts[-2], unquote(parts[-1])
                    payload = (fixture.blobs if kind == "blobs" else fixture.manifests)[
                        identity
                    ]
                    status, content_range = 200, None
                    if kind == "blobs" and self.headers.get("Range"):
                        import re
                        match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers["Range"])
                        if not match or not 0 <= int(match[1]) <= int(match[2]) < len(payload):
                            self.send_error(416)
                            return
                        start, end = map(int, match.groups())
                        content_range = f"bytes {start}-{end}/{len(payload)}"
                        payload = payload[start:end + 1]
                        status = 206
                    if kind == "blobs":
                        fixture.downloaded_bytes += len(payload)
                    self.send_response(status)
                    if content_range:
                        self.send_header("Content-Range", content_range)
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header(
                        "Content-Type",
                        OCI_IMAGE
                        if kind == "manifests"
                        else "application/octet-stream",
                    )
                    self.end_headers()
                    self.wfile.write(payload)
                except KeyError:
                    self.send_error(404)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        Thread(target=server.serve_forever, daemon=True).start()
        url = "http://127.0.0.1:" + str(server.server_port)

        def backend_command(directory):
            if not args.prefetch:
                return [sys.executable, "-m", "ucloud_sandboxes.cli", "serve-environment-io",
                        "--root", str(directory / "backend"), "--socket", str(directory / "backend.sock"),
                        "--environment-registry-url", url, "--environment-registry-repository", "environments",
                        "--environment-trusted-keys", str(directory / "keys.json")]
            (directory / "backend.json").write_text(json.dumps({
                "root": str(directory / "backend"), "socket": str(directory / "backend.sock"), "url": url,
                "keys": str(directory / "keys.json"), "metrics": str(directory / "metrics.json"),
                "trace_window_seconds": 8.0}))
            return [sys.executable, str(Path(__file__).resolve()), "--metered-backend",
                    str(directory / "backend.json")]

        backend = start_backend(backend_command(root), root / "backend.log", root / "backend.sock")
        config = {
            "ociVersion": "1.0.2",
            "root": {"path": "rootfs", "readonly": False},
            "process": {
                "terminal": False,
                "user": {"uid": 0, "gid": 0},
                "args": [
                    "/bin/busybox",
                    "sh",
                    "-ec",
                    "test ! -e /delete-me; test ! -e /opaque/old; test -e /opaque/new; "
                    "test /etc/hello -ef /etc/hardlink; "
                    "echo GUEST_READY; /bin/busybox sleep 3; "
                    'test "$(/bin/busybox cat /etc/symlink)" = immutable; '
                    'echo guest-copy-up > /etc/hello; test "$(/bin/busybox cat /etc/hello)" = guest-copy-up; echo GUEST_OK',
                ],
                "env": ["PATH=/bin"],
                "cwd": "/",
                "noNewPrivileges": True,
                "capabilities": {
                    name: []
                    for name in ("bounding", "effective", "inheritable", "permitted")
                },
            },
            "mounts": [
                {
                    "destination": "/proc",
                    "type": "proc",
                    "source": "proc",
                    "options": ["nosuid", "noexec", "nodev"],
                }
            ],
            "linux": {
                "namespaces": [
                    {"type": kind} for kind in ("pid", "ipc", "uts", "mount", "network")
                ]
            },
        }
        settings = {
            "root": str(root),
            "url": url,
            "image_ref": url[7:] + "/environments:image@" + image_digest,
            "config": config,
        }
        settings_path = root / "frontend.json"
        settings_path.write_text(json.dumps(settings))
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--frontend",
            str(settings_path),
        ]
        started = time.monotonic()
        created = subprocess.run(
            command, check=True, capture_output=True, text=True, timeout=30
        )
        metadata = json.loads(created.stdout)
        result["cold_materialize_seconds"] = time.monotonic() - started
        result["cold_materialize_blob_bytes"] = fixture.downloaded_bytes
        if args.prefetch:
            result["prefetch_after_materialize"] = prefetch_metrics(root / "metrics.json")
            assert result["prefetch_after_materialize"]["metadata_hint_present"] == len(components)
        identifier = metadata["container_id"]
        bundle = Path(metadata["bundle"])
        merged = bundle / "rootfs"
        assert (merged / "etc/symlink").read_text() == "immutable\n"
        assert os.getxattr(merged / "etc/hello", "user.fixture") == b"preserved"
        assert (
            not (merged / "delete-me").exists() and not (merged / "opaque/old").exists()
        )
        assert (merged / "opaque/new").read_text() == "visible"
        guest, result["guest_ready_seconds"] = start_guest(runsc, bundle, identifier, root)
        settings.update(
            image_id=metadata["image_id"], fingerprint=metadata["fingerprint"]
        )
        settings_path.write_text(json.dumps(settings))
        before_restart_bytes = fixture.downloaded_bytes
        restart_started = time.monotonic()
        restarted = subprocess.run(
            command, check=True, capture_output=True, text=True, timeout=30
        )
        result["frontend_replacement_seconds"] = time.monotonic() - restart_started
        result["frontend_replacement_blob_bytes"] = (
            fixture.downloaded_bytes - before_restart_bytes
        )
        assert Path(json.loads(restarted.stdout)["rootfs"]).is_dir()
        assert guest.poll() is None
        client = EnvironmentBackendClient(root / "backend.sock")
        assert all(not client.drop(digest) for digest in components)
        assert len(
            {
                (root / "backend/components" / digest[7:]).stat().st_dev
                for digest in components
            }
        ) == len(components)
        finish_guest(guest, root)
        if args.prefetch:
            wait_for(lambda: prefetch_metrics(root / "metrics.json")["traces_recorded"] == len(components),
                     30, "startup trace recording")
            result["prefetch_after_guest"] = prefetch_metrics(root / "metrics.json")
        # Default gVisor root:self keeps guest writes in its disk-backed upper;
        # independently qualify the canonical host overlay's copy-up semantics.
        assert (merged / "etc/hello").read_text() == "immutable\n"
        (merged / "etc/hello").write_text("copy-up\n")
        assert os.getxattr(merged / "etc/hello", "user.fixture") == b"preserved"
        result.update(
            image_bytes=total_bytes,
            http_blob_bytes=fixture.downloaded_bytes,
            frontend_process_replacement=True,
            live_guest=True,
            whiteout=True,
            opaque_directory=True,
            hardlinks=True,
            xattrs=True,
            writable_copyup=True,
            retained_lower_gc_fence=True,
        )
        assert fixture.downloaded_bytes < total_bytes // 8
        if args.layers:
            from qualify_environment_layers import qualify_layers
            result["layers"] = qualify_layers(root, source, fixture, registry, key, client, url, runsc, config)
        # Deliberate artifact-backend loss after the guest exits. A fresh
        # process must fence retained mounts, never attach a replacement NBD
        # underneath an old filesystem and pretend it recovered safely.
        backend.kill()
        backend.wait(timeout=10)
        replacement = subprocess.run(
            backend.args, capture_output=True, text=True, timeout=10
        )
        assert (
            replacement.returncode != 0
            and "lost with retained mounts" in replacement.stderr
        )
        result["backend_loss_fenced"] = True
        if args.prefetch:
            # C2.3 replay: a fresh backend and chunk cache that keep only the
            # first run's startup traces. Release the first run's mounts so
            # no new export can bind a device under its filesystems.
            subprocess.run(runsc + ["delete", "--force", identifier], capture_output=True, timeout=20)
            if unmount_under(root):
                raise RuntimeError("could not release the first run's mounts")
            replay = root / "replay"
            replay.mkdir(mode=0o700)
            shutil.copy2(root / "keys.json", replay / "keys.json")
            shutil.copytree(root / "backend/traces", replay / "backend/traces")
            (replay / "backend").chmod(0o700)
            traces = len(list((replay / "backend/traces").glob("*.json")))
            backend = start_backend(backend_command(replay), replay / "backend.log", replay / "backend.sock")
            (replay / "frontend.json").write_text(json.dumps(
                {"root": str(replay), "url": url, "image_ref": settings["image_ref"], "config": config}))
            before, started = fixture.downloaded_bytes, time.monotonic()
            created = subprocess.run(command[:-1] + [str(replay / "frontend.json")], check=True,
                                     capture_output=True, text=True, timeout=30)
            replay_result = {"traces": traces, "cold_materialize_seconds": time.monotonic() - started,
                             "cold_materialize_blob_bytes": fixture.downloaded_bytes - before,
                             "after_materialize": prefetch_metrics(replay / "metrics.json")}
            metadata = json.loads(created.stdout)
            identifier = metadata["container_id"]
            guest, replay_result["guest_ready_seconds"] = start_guest(
                runsc, Path(metadata["bundle"]), identifier, replay)
            finish_guest(guest, replay)
            replay_result.update(http_blob_bytes=fixture.downloaded_bytes - before,
                                 after_guest=prefetch_metrics(replay / "metrics.json"))
            result["trace_replay"] = replay_result
            assert traces == len(components) == replay_result["after_materialize"]["trace_hint_present"]
            assert replay_result["after_guest"]["trace_prefetch_chunks"] > 0
        result["passed"] = True
    except Exception as exc:
        result["passed"] = False
        result["error"] = repr(exc)
        result["debug_tail"] = {
            path.name: path.read_text(errors="replace")[-5000:]
            for path in root.glob("runsc-debug-*")
        }
        if isinstance(exc, subprocess.CalledProcessError):
            result["stderr"] = exc.stderr
    finally:
        if identifier:
            subprocess.run(
                runsc + ["delete", "--force", identifier],
                capture_output=True,
                timeout=20,
            )
        if guest is not None and guest.poll() is None:
            guest.kill()
            guest.wait()
        cleanup = unmount_under(root)
        if backend is not None:
            backend.terminate()
            backend.wait(timeout=10)
        if server is not None:
            server.shutdown()
            server.server_close()
        result["cleanup_errors"] = cleanup
        if cleanup:
            result["passed"] = False
            result["retained_fixture"] = str(root)
        else:
            shutil.rmtree(root)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
