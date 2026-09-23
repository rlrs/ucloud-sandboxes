#!/usr/bin/env python3
"""Isolated Linux EROFS/NBD qualification; never targets production state.

Exercises real OCI HTTP reads, signature/chunk verification, composition,
OverlayRootfsManager and a live gVisor guest across frontend process replacement.
The in-process registry is a fixture: this does not claim WAN latency results.
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
from ucloud_sandboxes.environment_backend import EnvironmentBackendClient
from ucloud_sandboxes.environment_builder import allowlisted_build_view
from ucloud_sandboxes.environment_config import configured_environment_registry
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.environment_rootfs import EnvironmentRootfsStore
from ucloud_sandboxes.image_rootfs import OverlayRootfsManager


class FixtureRegistry:
    def __init__(self):
        self.blobs, self.manifests, self.uploads = {}, {}, {}
        self.downloaded_bytes = 0

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
        return json.loads(self.manifests[digest]), {}

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runsc", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--frontend", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.frontend:
        frontend(args.frontend)
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
            components.append(registry.publish(image, component, tag=name))
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
                    if kind == "blobs":
                        fixture.downloaded_bytes += len(payload)
                    self.send_response(200)
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
        log = (root / "backend.log").open("w")
        backend = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "ucloud_sandboxes.environment_backend",
                "--root",
                str(root / "backend"),
                "--socket",
                str(root / "backend.sock"),
                "--registry-url",
                url,
                "--repository",
                "environments",
                "--trusted-keys",
                str(root / "keys.json"),
            ],
            stdout=log,
            stderr=log,
        )
        deadline = time.monotonic() + 10
        while not (root / "backend.sock").exists():
            if backend.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(
                    "backend failed to start: " + (root / "backend.log").read_text()
                )
            time.sleep(0.02)
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
        identifier = metadata["container_id"]
        bundle = Path(metadata["bundle"])
        merged = bundle / "rootfs"
        assert (merged / "etc/symlink").read_text() == "immutable\n"
        assert os.getxattr(merged / "etc/hello", "user.fixture") == b"preserved"
        assert (
            not (merged / "delete-me").exists() and not (merged / "opaque/old").exists()
        )
        assert (merged / "opaque/new").read_text() == "visible"
        guest = subprocess.Popen(
            runsc + ["run", "--bundle=" + str(bundle), identifier],
            stdout=(root / "guest.stdout").open("w"),
            stderr=(root / "guest.stderr").open("w"),
            text=True,
        )
        deadline = time.monotonic() + 15
        while "GUEST_READY" not in (root / "guest.stdout").read_text():
            if guest.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(
                    "guest failed to become live: "
                    + (root / "guest.stderr").read_text()
                )
            time.sleep(0.05)
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
        guest.wait(timeout=10)
        stdout, stderr = (
            (root / "guest.stdout").read_text(),
            (root / "guest.stderr").read_text(),
        )
        assert guest.returncode == 0 and "GUEST_OK" in stdout, stderr
        # Default gVisor root:self keeps guest writes in its disk-backed upper;
        # independently qualify the canonical host overlay's copy-up semantics.
        assert (merged / "etc/hello").read_text() == "immutable\n"
        (merged / "etc/hello").write_text("copy-up\n")
        assert os.getxattr(merged / "etc/hello", "user.fixture") == b"preserved"
        result.update(
            passed=True,
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
    except Exception as exc:
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
        # Only mounts under this freshly allocated qualification directory.
        mounts = [
            line.split()[4].replace("\\040", " ")
            for line in Path("/proc/self/mountinfo").read_text().splitlines()
            if line.split()[4].startswith(str(root) + "/")
        ]
        cleanup = []
        for mount in sorted(
            mounts, key=lambda value: (value.count("/"), value), reverse=True
        ):
            completed = subprocess.run(
                ["umount", mount], capture_output=True, text=True, timeout=20
            )
            if completed.returncode:
                cleanup.append(completed.stderr)
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
