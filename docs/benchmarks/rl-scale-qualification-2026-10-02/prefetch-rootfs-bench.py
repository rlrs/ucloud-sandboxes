#!/usr/bin/env python3
"""C2.2/C2.3 on a real scientific-Python rootfs over real NBD/EROFS (qualification only).

One component from /srv/spike/rootfs (mkfs.erofs -T0 --mkfs-time -U null
-zlz4, the C2.11 flags), published twice: with and without its signed
metadata hint. A fixture HTTP registry adds a fixed delay per blob request.
Each run starts the metered artifact backend on a fresh root and cache, then
from separate processes: ensure (attach + mount + metadata wait), a full
metadata walk (find -xdev, every lstat), and the first command, a chroot
python3 import of numpy, pandas and scipy. Modes: plain (no hint), hint,
hint+trace (traces copied from the hint run).
"""
import argparse
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
from urllib.parse import unquote

sys.path.insert(0, "/root/qual/ucloud-sandboxes")
sys.path.insert(0, "/root/qual/ucloud-sandboxes/runtime/storage_native")
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat  # noqa: E402

from qualify_environment import FixtureRegistry, PREFETCH_METRICS  # noqa: E402
from ucloud_sandboxes.environment_artifact import EnvironmentArtifactRegistry, content_digest, sign_component  # noqa: E402,E501
from ucloud_sandboxes.environment_backend import EnvironmentBackendClient  # noqa: E402
from ucloud_sandboxes.environment_builder import WHOLE_IMAGE_EXCLUDED  # noqa: E402
from ucloud_sandboxes.environment_metadata import sign_metadata_hint  # noqa: E402

QUALIFY = "/root/qual/ucloud-sandboxes/runtime/storage_native/qualify_environment.py"
IMPORT = "import numpy, pandas, scipy.linalg, scipy.sparse, scipy.stats, scipy.optimize"


def serve(fixture, delay, log):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parts = self.path.split("/")
            kind, identity = parts[-2], unquote(parts[-1])
            try:
                payload = (fixture.blobs if kind == "blobs" else fixture.manifests)[identity]
            except KeyError:
                self.send_error(404)
                return
            status, extra = 200, {}
            if kind == "blobs":
                time.sleep(delay)
                if self.headers.get("Range"):
                    start, end = map(int, re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers["Range"]).groups())
                    extra["Content-Range"] = f"bytes {start}-{end}/{len(payload)}"
                    payload, status = payload[start:end + 1], 206
                with log["lock"]:
                    log["requests"] += 1
                    log["bytes"] += len(payload)
            self.send_response(status)
            for key, value in extra.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def metrics(path):
    time.sleep(0.1)
    values = json.loads(path.read_text())
    return {name: values[name] for name in PREFETCH_METRICS}


def run(args, work, url, digest, mode, delay, log, traces=None):
    root = work / f"{mode}-{int(delay * 1000)}ms"
    root.mkdir(mode=0o700)
    shutil.copy2(work / "keys.json", root / "keys.json")
    if traces is not None:
        shutil.copytree(traces, root / "backend/traces")
        (root / "backend").chmod(0o700)
    (root / "backend.json").write_text(json.dumps({
        "root": str(root / "backend"), "socket": str(root / "backend.sock"), "url": url,
        "keys": str(root / "keys.json"), "metrics": str(root / "metrics.json"), "trace_window_seconds": 20.0}))
    subprocess.run(["sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], check=True)
    backend = subprocess.Popen([sys.executable, QUALIFY, "--metered-backend", str(root / "backend.json")],
                               stdout=(root / "backend.log").open("w"), stderr=subprocess.STDOUT)
    result = {"mode": mode, "delay_ms": delay * 1000}
    try:
        while not (root / "backend.sock").exists():
            if backend.poll() is not None:
                raise RuntimeError((root / "backend.log").read_text())
            time.sleep(0.02)
        client = EnvironmentBackendClient(root / "backend.sock")
        before = dict(log)
        started = time.monotonic()
        mount = client.ensure(digest)
        result["ensure_seconds"] = time.monotonic() - started
        result["ensure_requests"] = log["requests"] - before["requests"]
        result["ensure_bytes"] = log["bytes"] - before["bytes"]
        result["after_ensure"] = metrics(root / "metrics.json")
        before = dict(log)
        started = time.monotonic()
        found = subprocess.run(["find", str(mount), "-xdev", "-printf", "%m %s %y\n"], capture_output=True,
                               check=True).stdout.count(b"\n")
        result["find"] = {"seconds": time.monotonic() - started, "entries": found,
                          "requests": log["requests"] - before["requests"],
                          "bytes": log["bytes"] - before["bytes"]}
        before = dict(log)
        started = time.monotonic()
        subprocess.run(["chroot", str(mount), "/usr/local/bin/python3", "-c", IMPORT], check=True,
                       env={"PATH": "/usr/local/bin:/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"})
        result["import"] = {"seconds": time.monotonic() - started, "requests": log["requests"] - before["requests"],
                            "bytes": log["bytes"] - before["bytes"]}
        result["after_import"] = metrics(root / "metrics.json")
        if mode == "hint":
            deadline = time.monotonic() + 40
            while metrics(root / "metrics.json")["traces_recorded"] < 1:
                if time.monotonic() > deadline:
                    raise RuntimeError("no startup trace was recorded")
                time.sleep(0.2)
            result["trace_chunks_recorded"] = metrics(root / "metrics.json")["trace_chunks_recorded"]
        if not client.drop(digest):
            raise RuntimeError("component still in use")
    finally:
        backend.terminate()
        backend.wait(timeout=10)
    return result, root / "backend/traces"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=Path("/srv/spike/rootfs"))
    parser.add_argument("--work", type=Path, default=Path("/var/lib/rl-spike/qual-prefetch"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--delays-ms", default="0,20")
    parser.add_argument("--mkfs-option", action="append", default=[], help="Extra mkfs.erofs option, e.g. --MZ")
    args = parser.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    args.work.mkdir(parents=True, mode=0o700)
    image = args.work / "rootfs.erofs"
    subprocess.run(["mkfs.erofs", "-T", "0", "--mkfs-time", "-U", "00000000-0000-0000-0000-000000000000", "-zlz4",
                    "--exclude-regex=^(" + "|".join(sorted(WHOLE_IMAGE_EXCLUDED)) + ")$", *args.mkfs_option, str(image),
                    str(args.source)], check=True, capture_output=True)
    key = Ed25519PrivateKey.generate()
    public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    keys = {content_digest(public): public}
    (args.work / "keys.json").write_text(json.dumps({k: base64.b64encode(v).decode() for k, v in keys.items()}))
    fixture = FixtureRegistry()
    registry = EnvironmentArtifactRegistry(fixture, "environments", keys)
    component = sign_component(image, source_image="sha256:" + "1" * 64, signing_key=key)
    hint, walked = sign_metadata_hint(image, component, key)
    digests = {"plain": registry.publish(image, component, tag="plain"),
               "hint": registry.publish(image, component, tag="hint", metadata=hint)}
    report = {"mkfs_options": args.mkfs_option, "image_bytes": image.stat().st_size, "image_sha256": hashlib.sha256(image.read_bytes()).hexdigest(),
              "chunks": len(component.chunks), "hint_chunks": len(hint.chunks),
              "metadata_bytes": walked.metadata_bytes, "inodes": walked.inodes, "runs": []}
    log = {"requests": 0, "bytes": 0, "lock": threading.Lock()}
    try:
        for delay in (int(value) / 1000 for value in args.delays_ms.split(",")):
            server = serve(fixture, delay, log)
            url = "http://127.0.0.1:" + str(server.server_port)
            try:
                plain, _ = run(args, args.work, url, digests["plain"], "plain", delay, log)
                hinted, traces = run(args, args.work, url, digests["hint"], "hint", delay, log)
                replay, _ = run(args, args.work, url, digests["hint"], "hint+trace", delay, log, traces=traces)
                report["runs"] += [plain, hinted, replay]
            finally:
                server.shutdown()
                server.server_close()
            args.output.write_text(json.dumps(report, indent=1, default=str) + "\n")
            print("delay", delay, "done", flush=True)
    finally:
        args.output.write_text(json.dumps(report, indent=1, default=str) + "\n")
        shutil.rmtree(args.work, ignore_errors=True)


if __name__ == "__main__":
    main()
