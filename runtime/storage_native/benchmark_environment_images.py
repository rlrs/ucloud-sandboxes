#!/usr/bin/env python3
"""Compare image adapters on an owned Linux VM, using identical live guests.

A local OCI registry supplies both paths. Docker gets a fresh private daemon
image store; EROFS gets a fresh verified chunk cache. Host page caches are never
flushed. Timings are local-registry first-use measurements, not WAN or wake SLOs.
"""

from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from threading import Lock, Thread
import time
import uuid

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from ucloud_sandboxes.environment_artifact import load_image_environment
from ucloud_sandboxes.environment_backend import EnvironmentBackendClient
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder
from ucloud_sandboxes.environment_config import configured_environment_registry
from ucloud_sandboxes.environment_keys import provision as provision_key
from ucloud_sandboxes.environment_rootfs import EnvironmentRootfsStore
from ucloud_sandboxes.image_rootfs import (
    DockerOverlay2RootfsStore,
    OverlayRootfsManager,
)

IMAGES = (
    ("python-slim", "python:3.13-slim-bookworm", "python"),
    ("node-slim", "node:22-bookworm-slim", "node"),
    ("python-tools", "python:3.13-bookworm", "python"),
    ("node-tools", "node:22-bookworm", "node"),
)
ALLOWLIST = (
    "bin",
    "boot",
    "dev",
    "etc",
    "home",
    "lib",
    "lib64",
    "media",
    "mnt",
    "opt",
    "proc",
    "root",
    "run",
    "sbin",
    "srv",
    "sys",
    "tmp",
    "usr",
    "var",
    "repo",
)
PYTHON_WORK = r"""import hashlib,json,pathlib,sqlite3,ssl,subprocess
files=sorted(pathlib.Path('/repo').rglob('*.py'))[:128]
h=hashlib.sha256();size=0
for p in files:
 data=p.read_bytes();size+=len(data);h.update(data)
assert len(files)==128
ssl.create_default_context()
db=sqlite3.connect('/tmp/test.sqlite');db.execute('create table result (n)');db.execute('insert into result values (42)');db.commit();assert db.execute('select n from result').fetchone()[0]==42
db.close();pathlib.Path('/tmp/copyup').write_text(h.hexdigest());assert pathlib.Path('/tmp/copyup').read_text()==h.hexdigest()
assert subprocess.check_output(['/bin/sh','-c','printf ok'])==b'ok'
print(json.dumps({'files':len(files),'bytes':size,'digest':h.hexdigest(),'copyup':True},sort_keys=True))
"""
NODE_WORK = r"""const fs=require('fs'),crypto=require('crypto'),cp=require('child_process');
function walk(p){return fs.readdirSync(p,{withFileTypes:true}).flatMap(x=>x.isDirectory()?walk(p+'/'+x.name):x.isFile()&&x.name.endsWith('.py')?[p+'/'+x.name]:[])}
let files=walk('/repo').sort().slice(0,128),size=0,h=crypto.createHash('sha256');
for(let p of files){let d=fs.readFileSync(p);size+=d.length;h.update(d)}
if(files.length!==128)throw Error('files');let digest=h.digest('hex');fs.writeFileSync('/tmp/copyup',digest);if(fs.readFileSync('/tmp/copyup','utf8')!==digest)throw Error('copyup');if(cp.execFileSync('/bin/sh',['-c','printf ok']).toString()!=='ok')throw Error('child');
console.log(JSON.stringify({files:files.length,bytes:size,digest,copyup:true}));
"""


def run(*command, timeout=600, **kwargs):
    result = subprocess.run(
        command, capture_output=True, text=True, timeout=timeout, **kwargs
    )
    if result.returncode:
        raise RuntimeError(
            f"{command[0]} exited {result.returncode}: {result.stderr[-4000:]}"
        )
    return result.stdout


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class CountingRegistry:
    def __init__(self, port):
        self.port, self.lock, self.bytes, self.requests = port, Lock(), 0, 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                self.proxy(False)

            def do_HEAD(self):
                self.proxy(True)

            def proxy(self, head):
                connection = HTTPConnection("127.0.0.1", outer.port, timeout=60)
                try:
                    connection.request(
                        "HEAD" if head else "GET",
                        self.path,
                        headers={
                            k: v
                            for k, v in self.headers.items()
                            if k.lower() not in {"host", "connection"}
                        },
                    )
                    response = connection.getresponse()
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in {
                            "connection",
                            "transfer-encoding",
                            "server",
                            "date",
                        }:
                            self.send_header(key, value)
                    self.send_header("Connection", "close")
                    self.end_headers()
                    if not head:
                        while chunk := response.read(256 * 1024):
                            self.wfile.write(chunk)
                            if "/blobs/" in self.path and response.status in (200, 206):
                                with outer.lock:
                                    outer.bytes += len(chunk)
                    with outer.lock:
                        outer.requests += 1
                finally:
                    connection.close()
                    self.close_connection = True

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def snapshot(self):
        with self.lock:
            return self.bytes, self.requests

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def rss_bytes(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except FileNotFoundError:
        pass
    return None


@contextmanager
def private_docker(root):
    root.mkdir()
    daemon_config = root / "daemon.json"
    daemon_config.write_text(
        json.dumps({"features": {"containerd-snapshotter": False}})
    )
    sock = root / "docker.sock"
    log = (root / "daemon.log").open("w")
    daemon = subprocess.Popen(
        [
            "dockerd",
            "--config-file",
            str(daemon_config),
            "--data-root",
            str(root / "data"),
            "--exec-root",
            str(root / "exec"),
            "--host",
            f"unix://{sock}",
            "--pidfile",
            str(root / "docker.pid"),
            "--bridge",
            "none",
            "--iptables=false",
            "--ip6tables=false",
            "--ip-forward=false",
            "--ip-masq=false",
            "--storage-driver",
            "overlay2",
            "--containerd-namespace",
            "p4-" + root.parent.name,
        ],
        stdout=log,
        stderr=log,
    )
    wrapper = root / "docker"
    wrapper.write_text(
        "#!/bin/sh\nexec /usr/bin/docker --host unix://" + str(sock) + ' "$@"\n'
    )
    wrapper.chmod(0o700)
    try:
        deadline = time.monotonic() + 30
        while True:
            if daemon.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(
                    "private Docker failed: "
                    + (root / "daemon.log").read_text()[-3000:]
                )
            if (
                sock.exists()
                and subprocess.run(
                    [str(wrapper), "info"], capture_output=True
                ).returncode
                == 0
            ):
                break
            time.sleep(0.1)
        yield str(wrapper), daemon
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=30)
        except subprocess.TimeoutExpired:
            daemon.kill()
            daemon.wait()
        log.close()


@contextmanager
def artifact_backend(root, url, keys):
    root.mkdir()
    log = (root / "backend.log").open("w")
    sock = root / "backend.sock"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ucloud_sandboxes.environment_backend",
            "--root",
            str(root / "io"),
            "--socket",
            str(sock),
            "--registry-url",
            url,
            "--repository",
            "environments",
            "--trusted-keys",
            str(keys),
            "--cache-bytes",
            str(256 * 1024**2),
        ],
        stdout=log,
        stderr=log,
    )
    try:
        deadline = time.monotonic() + 20
        while not sock.exists():
            if process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(
                    "artifact backend failed: "
                    + (root / "backend.log").read_text()[-3000:]
                )
            time.sleep(0.05)
        yield EnvironmentBackendClient(sock), process
    finally:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        log.close()


def guest(store, ref, kind, root, runsc, sequence):
    config = {
        "ociVersion": "1.0.2",
        "root": {"path": "rootfs", "readonly": False},
        "process": {
            "terminal": False,
            "user": {"uid": 0, "gid": 0},
            "args": ["/usr/local/bin/python", "-c", PYTHON_WORK]
            if kind == "python"
            else ["/usr/local/bin/node", "-e", NODE_WORK],
            "env": ["PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8"],
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
                {"type": name} for name in ("pid", "ipc", "uts", "mount", "network")
            ]
        },
    }
    manager = OverlayRootfsManager(
        store, writable_root=root / "writable", bundle_root=root / "bundles"
    )
    started = time.monotonic()
    lease = None
    memory_directory = None
    image_id = None
    runtime = [
        str(runsc),
        "--root=" + str(root / "runsc"),
        "--platform=systrap",
        "--network=none",
        "--application-memory-file-dir=" + str(root.parent / "ram"),
    ]
    try:
        with store.operation_lease(ref) as image:
            image_id = image.image_id
            lease = manager.prepare(
                sandbox_id=f"probe-{sequence}",
                sandbox_generation=1,
                image=image,
                config_template=config,
            )
            memory_directory = root.parent / "ram" / lease.sandbox.memory_directory
            memory_directory.mkdir(mode=0o700)
            prepare_seconds = time.monotonic() - started
            timing = root / f"time-{sequence}.txt"
            output = run(
                "/usr/bin/time",
                "-f",
                "%M",
                "-o",
                str(timing),
                *runtime,
                "run",
                "--bundle=" + str(lease.sandbox.bundle),
                lease.sandbox.container_id,
                timeout=120,
            )
            useful = time.monotonic() - started
            proof = json.loads(output.strip().splitlines()[-1])
            assert proof["files"] == 128 and proof["copyup"]
            return {
                "materialize_seconds": prepare_seconds,
                "first_useful_seconds": useful,
                "runsc_command_peak_rss_bytes": int(timing.read_text().strip()) * 1024,
                "proof": proof,
                "image_id": image_id,
            }
    finally:
        if lease is not None:
            subprocess.run(
                runtime + ["delete", "--force", lease.sandbox.container_id],
                capture_output=True,
                timeout=30,
            )
            manager.release(lease)
        if memory_directory is not None:
            shutil.rmtree(memory_directory)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runsc", type=Path, required=True)
    p.add_argument("--repo", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--repeats", type=int, default=2)
    p.add_argument(
        "--resume-prepared",
        action="store_true",
        help="reuse this fixture's signed publications, with new adapter caches",
    )
    args = p.parse_args()
    previous = json.loads(args.output.read_text()) if args.resume_prepared else None
    if previous is not None and (
        not args.root.is_dir()
        or [item["name"] for item in previous["images"]] != [item[0] for item in IMAGES]
    ):
        p.error(
            "resume requires this fixture's complete four-image publication inventory"
        )
    if os.geteuid() != 0 or (args.root.exists() and previous is None):
        p.error("requires root and a new owned fixture root")
    args.root.mkdir(mode=0o700, exist_ok=previous is not None)
    root = args.root
    results = {
        "scope": "local OCI registry, empty adapter caches; host page caches retained",
        "runsc_sha256": hashlib.sha256(args.runsc.read_bytes()).hexdigest(),
        "kernel": os.uname().release,
        "images": [],
        "measurements": [],
        "cleanup_errors": [],
        "passed": False,
        "preparation_reused": args.resume_prepared,
    }
    if (
        results["runsc_sha256"]
        != "a005058b5a097a9ec6c28d0d3e14ea7d2992fc0cf2c11a0eb62d7553e7068613"
    ):
        raise ValueError("requires exact qualified six-patch runsc")
    registry_name = "ucloud-p4-heterogeneous-" + str(os.getpid())
    proxy = None
    tagged = []
    try:
        run("modprobe", "erofs")
        run("modprobe", "nbd", "nbds_max=64")
        (root / "ram").mkdir(exist_ok=True)
        run("mount", "-t", "tmpfs", "-o", "size=1G,noswap", "tmpfs", str(root / "ram"))
        port = free_port()
        url = f"http://127.0.0.1:{port}"
        run(
            "docker",
            "run",
            "-d",
            "--name",
            registry_name,
            "--network",
            "host",
            "-e",
            f"REGISTRY_HTTP_ADDR=127.0.0.1:{port}",
            "-v",
            str(root / "registry") + ":/var/lib/registry",
            "registry:2",
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                conn = HTTPConnection("127.0.0.1", port, timeout=1)
                conn.request("GET", "/v2/")
                status = conn.getresponse().status
                conn.close()
                if status == 200:
                    break
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError("registry startup")
            time.sleep(0.1)
        provision_key(root / "keys")
        keys = root / "keys/producers.json"
        key = load_pem_private_key((root / "keys/producer.pem").read_bytes(), None)
        registry = configured_environment_registry(url, "environments", keys)
        builder_store = DockerOverlay2RootfsStore(root / "builder-images")
        builder = FreshEnvironmentBuilder(
            builder_store, registry, key, root / "build-work"
        )
        context = root / "context"
        if previous is None:
            context.mkdir()
            shutil.copytree(
                args.repo,
                context / "repo",
                ignore=shutil.ignore_patterns(".git", ".venv", "__pycache__"),
            )
        prepared = list(previous["images"]) if previous else []
        results["images"] = prepared
        for name, base, kind in () if previous else IMAGES:
            print("BUILD " + name, flush=True)
            ref = f"127.0.0.1:{port}/images/{name}:qualification"
            tagged.append(ref)
            (context / "Dockerfile").write_text(f"FROM {base}\nCOPY repo /repo\n")
            run("docker", "build", "--pull", "-t", ref, str(context), timeout=900)
            base_identity = json.loads(run("docker", "image", "inspect", base))[0]
            run("docker", "push", ref, timeout=600)
            with builder_store.operation_lease(ref) as source:
                allow = [
                    path
                    for path in ALLOWLIST
                    if (source.rootfs / path).exists()
                    or (source.rootfs / path).is_symlink()
                ]
            started = time.monotonic()
            digest = builder.publish_image(ref, allowlist=allow)
            build_seconds = time.monotonic() - started
            _, environment = load_image_environment(registry, f"images/{name}", digest)
            artifact_bytes = sum(
                registry.load(component).image_size
                for component in environment.components
            )
            manifest, _ = registry.client.manifest_document(f"images/{name}", digest)
            metadata = {
                "name": name,
                "base": base,
                "base_image_id": base_identity["Id"],
                "base_repo_digests": base_identity.get("RepoDigests", []),
                "kind": kind,
                "digest": digest,
                "artifact_bytes": artifact_bytes,
                "oci_compressed_layer_bytes": sum(
                    layer["size"] for layer in manifest["layers"]
                ),
                "publication_seconds": build_seconds,
                "components": list(environment.components),
            }
            prepared.append(metadata)
            args.output.write_text(json.dumps(results, indent=2))
        proxy = CountingRegistry(port)
        trial_prefix = uuid.uuid4().hex[:8]
        for repeat in range(args.repeats):
            for item in prepared:
                for adapter in (
                    ("docker", "erofs") if repeat % 2 == 0 else ("erofs", "docker")
                ):
                    print(f"MEASURE {repeat} {item['name']} {adapter}", flush=True)
                    trial = root / f"{trial_prefix}-{repeat}-{item['name']}-{adapter}"
                    trial.mkdir()
                    (trial / "ram").symlink_to(root / "ram")
                    ref = (
                        proxy.url.removeprefix("http://")
                        + f"/images/{item['name']}@"
                        + item["digest"]
                    )
                    manager_context = (
                        private_docker(trial / "daemon")
                        if adapter == "docker"
                        else artifact_backend(trial / "backend", proxy.url, keys)
                    )
                    with manager_context as (backend, process):
                        store = (
                            DockerOverlay2RootfsStore(
                                trial / "images", docker_binary=backend
                            )
                            if adapter == "docker"
                            else EnvironmentRootfsStore(
                                trial / "images",
                                configured_environment_registry(
                                    proxy.url, "environments", keys
                                ),
                                backend,
                            )
                        )
                        image_id = None
                        for cache in ("cold", "warm"):
                            before, requests = proxy.snapshot()
                            started = time.monotonic()
                            pull = 0
                            if adapter == "docker" and cache == "cold":
                                run(backend, "pull", ref, timeout=600)
                                pull = time.monotonic() - started
                            row = guest(
                                store,
                                ref,
                                item["kind"],
                                trial / "guest",
                                args.runsc,
                                cache,
                            )
                            image_id = row.pop("image_id")
                            row.update(
                                {
                                    "adapter": adapter,
                                    "cache": cache,
                                    "repeat": repeat,
                                    "image": item["name"],
                                    "pull_seconds": pull,
                                    "first_useful_seconds": pull
                                    + row["first_useful_seconds"],
                                    "http_blob_bytes": proxy.snapshot()[0] - before,
                                    "http_requests": proxy.snapshot()[1] - requests,
                                    "adapter_process_rss_bytes": rss_bytes(process.pid),
                                }
                            )
                            results["measurements"].append(row)
                            args.output.write_text(json.dumps(results, indent=2))
                        assert store.collect_image(
                            image_id, is_referenced=lambda _: False
                        )
        for item in prepared:
            proofs = [
                row["proof"]
                for row in results["measurements"]
                if row["image"] == item["name"]
            ]
            assert all(proof == proofs[0] for proof in proofs)
        results["passed"] = True
    except Exception as exc:
        results["error"] = repr(exc)
        raise
    finally:
        if proxy:
            proxy.close()
        subprocess.run(
            ["docker", "rm", "-f", registry_name], capture_output=True, timeout=30
        )
        for ref in tagged:
            subprocess.run(
                ["docker", "image", "rm", ref], capture_output=True, timeout=30
            )
        mounts = [
            line.split()[4]
            for line in Path("/proc/self/mountinfo").read_text().splitlines()
            if line.split()[4].startswith(str(root) + "/")
        ]
        for mount in sorted(mounts, key=lambda v: (v.count("/"), v), reverse=True):
            result = subprocess.run(
                ["umount", mount], capture_output=True, text=True, timeout=30
            )
            if result.returncode:
                results["cleanup_errors"].append(result.stderr)
        if results["cleanup_errors"]:
            results["passed"] = False
        args.output.write_text(json.dumps(results, indent=2) + "\n")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
