#!/usr/bin/env python3
"""Bounded NAT and private-registry traffic from a separate diagnostic node.

Requires Python, curl, and ip. No credentials, image builds, global pruning, or
GC. Registry rate counts upload plus readback bytes. The shared byte ceiling
includes both directions and can end the test before its duration ceiling.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import math
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from uuid import uuid4


MIB = 1024 * 1024
GIB = 1024 * MIB
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"


class Budget:
    def __init__(self, limit, deadline):
        self.limit, self.deadline, self.reserved = limit, deadline, 0
        self.lock = threading.Lock()
        self.stopped = threading.Event()

    def reserve(self, count):
        with self.lock:
            if self.stopped.is_set() or time.monotonic() >= self.deadline or self.reserved + count > self.limit:
                return False
            self.reserved += count
            return True


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(MIB), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def write_blob(path, seed, size):
    digest, remaining, counter = hashlib.sha256(), size, 0
    with path.open("wb") as stream:
        while remaining:
            chunk = hashlib.shake_256(seed + counter.to_bytes(8, "big")).digest(min(MIB, remaining))
            stream.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
            counter += 1
    return "sha256:" + digest.hexdigest()


def upload_location(registry, repository, location, digest=None):
    resolved = urlsplit(urljoin(registry + "/", location))
    origin = urlsplit(registry)
    prefix = "/v2/" + repository + "/blobs/uploads/"
    if resolved.scheme != origin.scheme or resolved.netloc != origin.netloc or not resolved.path.startswith(prefix):
        raise ValueError("registry returned an upload location outside this fixture")
    if digest is None:
        return resolved.geturl()
    query = [(key, value) for key, value in parse_qsl(resolved.query, keep_blank_values=True) if key != "digest"]
    return urlunsplit(resolved._replace(query=urlencode(query + [("digest", digest)])))


def curl(url, directory, *, method="GET", source=None, limit=None, rate=None, timeout=15, headers=()):
    """Never export stderr or response bodies, which could contain opaque state."""
    identity = uuid4().hex
    body, header_file = directory / (identity + ".body"), directory / (identity + ".headers")
    command = ["curl", "-4", "--silent", "--show-error", "--fail", "--connect-timeout", "5",
               "--max-time", str(max(.1, timeout)), "--request", method,
               "--dump-header", str(header_file), "--output", str(body),
               "--write-out", "%{http_code}\n%{size_upload}\n%{size_download}\n"]
    if method == "HEAD":
        command.append("--head")
    if source is not None:
        command.extend(["--data-binary", "@" + str(source)])
    if limit is not None:
        command.extend(["--max-filesize", str(limit)])
    if rate is not None:
        command.extend(["--limit-rate", str(max(1, int(rate)))])
    for header in headers:
        command.extend(["--header", header])
    command.append(url)
    started = time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=max(.1, timeout) + 2)
    except subprocess.SubprocessError as exc:
        body.unlink(missing_ok=True)
        header_file.unlink(missing_ok=True)
        raise RuntimeError("curl subprocess failed: " + type(exc).__name__) from None
    fields = result.stdout.splitlines()
    status = int(fields[-3]) if len(fields) >= 3 and fields[-3].isdigit() else 0
    parsed = {}
    if header_file.exists():
        for line in header_file.read_text().splitlines():
            if line.startswith("HTTP/"):
                parsed = {}
            elif ":" in line:
                name, value = line.split(":", 1)
                parsed[name.lower()] = value.strip()
        header_file.unlink()
    if result.returncode or status >= 400:
        body.unlink(missing_ok=True)
        raise RuntimeError("curl failed: exit=" + str(result.returncode) + " http=" + str(status))
    return {"status": status, "headers": parsed, "body": body,
            "upload_bytes": int(float(fields[-2])), "download_bytes": int(float(fields[-1])),
            "seconds": time.monotonic() - started}


def registry_manifest(blob_digest, blob_size, run_id, index):
    config = b"{}"
    document = {"schemaVersion": 2, "mediaType": OCI_MANIFEST,
                "artifactType": "application/vnd.ucloud.gateway-network-probe.v1",
                "config": {"mediaType": "application/vnd.oci.empty.v1+json", "size": len(config),
                           "digest": "sha256:" + hashlib.sha256(config).hexdigest()},
                "layers": [{"mediaType": "application/octet-stream", "size": blob_size, "digest": blob_digest}],
                "annotations": {"org.ucloud.gateway-probe.run": run_id, "org.ucloud.gateway-probe.index": str(index)}}
    return config, json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def run(args):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:12]
    repository = "ucloud-diagnostics/gateway-mixed-" + run_id.lower()
    report = {"run_id": run_id, "repository": repository, "correct": False, "events": [], "errors": [],
              "cleanup_errors": [], "limits": {"duration_seconds": args.duration, "payload_bytes": int(args.max_gib * GIB),
                  "nat_mibps": args.nat_mibps, "registry_upload_plus_read_mibps": args.registry_mibps},
              "notes": ["Synthetic transport fixture; no actual image build or EROFS publication.",
                        "Payload reservations include upload and readback; protocol headers are additional.",
                        "Only fixture manifests and incomplete uploads are deleted; committed blobs await normal registry GC.",
                        "HTTPS terminates at the public test server: this exercises gateway NAT, not gateway TLS termination."]}
    lock = threading.Lock()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    event_path = args.output.with_suffix(args.output.suffix + ".events.jsonl")
    if args.output.exists():
        raise FileExistsError("refusing to overwrite an existing report")
    events = event_path.open("x")

    def event(name, **values):
        row = {"event": name, "at": datetime.now(timezone.utc).isoformat(),
               "elapsed_seconds": time.monotonic() - started, **values}
        with lock:
            report["events"].append(row)
            events.write(json.dumps(row) + "\n")
            events.flush()

    with tempfile.TemporaryDirectory(prefix="gateway-mixed-") as temporary:
        root = Path(temporary)
        registry = args.registry.rstrip("/")
        # A private source route plus verified public egress prevents silently
        # benchmarking the diagnostic node's own public NIC or the gateway itself.
        remote_ip = socket.gethostbyname(urlsplit(args.nat_url).hostname)
        route_result = subprocess.run(["ip", "-j", "route", "get", remote_ip], check=True,
                                      capture_output=True, text=True, timeout=5)
        route = json.loads(route_result.stdout)[0]
        source_ip = route.get("prefsrc", route.get("src", ""))
        if not ipaddress.ip_address(source_ip).is_private or source_ip == urlsplit(registry).hostname:
            raise RuntimeError("probe requires a separate node with a private IPv4 source route")
        external = curl("https://ip.hetzner.com", root, limit=1024, timeout=10)
        observed = external["body"].read_text().strip()
        external["body"].unlink()
        if observed != args.expected_egress_ip:
            raise RuntimeError("diagnostic node is not using the expected gateway public IPv4")
        head = curl(args.nat_url, root, method="HEAD", timeout=10)
        nat_size = int(head["headers"].get("content-length", "0"))
        head["body"].unlink(missing_ok=True)
        if not 0 < nat_size <= GIB:
            raise RuntimeError("NAT test object must advertise a size between one byte and 1 GiB")
        started = time.monotonic()
        deadline = started + args.duration
        work_deadline = deadline - 10
        maximum = int(args.max_gib * GIB)
        nat_budget = int(maximum * args.nat_mibps / (args.nat_mibps + args.registry_mibps))
        budgets = {"nat": Budget(nat_budget, work_deadline), "registry": Budget(maximum - nat_budget, work_deadline)}

        def stop(_signum, _frame):
            for budget in budgets.values():
                budget.stopped.set()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        event("traffic_started", private_source=source_ip, observed_egress_ipv4=observed,
              nat_object_bytes=nat_size, nat_payload_budget=nat_budget, registry_payload_budget=maximum - nat_budget)
        nat_digest = [None]

        def remaining(cleanup=False):
            return max(.1, min(30, (deadline if cleanup else work_deadline) - time.monotonic()))

        def failure(plane, exc, cleanup=False):
            # Exceptions are locally constructed status messages, not curl bodies.
            with lock:
                report["cleanup_errors" if cleanup else "errors"].append({"plane": plane, "error": str(exc)})
            for budget in budgets.values():
                budget.stopped.set()

        def nat_worker(worker):
            directory = root / ("nat-" + str(worker))
            directory.mkdir()
            estimated = nat_size / (args.nat_mibps * MIB / args.nat_workers) + 5
            while time.monotonic() + estimated < work_deadline and budgets["nat"].reserve(nat_size + 4096):
                try:
                    response = curl(args.nat_url, directory, limit=nat_size,
                                    rate=args.nat_mibps * MIB / args.nat_workers, timeout=remaining())
                    size, digest = response["body"].stat().st_size, digest_file(response["body"])
                    response["body"].unlink()
                    if size != nat_size:
                        raise RuntimeError("NAT object length mismatch")
                    with lock:
                        if nat_digest[0] is None:
                            nat_digest[0] = digest
                        if digest != nat_digest[0]:
                            raise RuntimeError("NAT object changed between downloads")
                    event("nat_download", bytes=size, sha256=digest, seconds=response["seconds"])
                except Exception as exc:
                    failure("nat", exc)
                    return

        def registry_worker(worker):
            directory = root / ("registry-" + str(worker))
            directory.mkdir()
            index, blob_size = worker, int(args.blob_mib * MIB)
            rate = args.registry_mibps * MIB / args.registry_workers
            estimated = 2 * blob_size / rate + 5
            while time.monotonic() + estimated < work_deadline and budgets["registry"].reserve(2 * blob_size + 8192):
                upload_urls, manifest_digest, committed = [], None, False
                fixture_started = time.monotonic()
                try:
                    blob = directory / "blob"
                    blob_digest = write_blob(blob, (run_id + ":" + str(index)).encode(), blob_size)
                    config, manifest = registry_manifest(blob_digest, blob_size, run_id, index)
                    config_file, manifest_file = directory / "config", directory / "manifest"
                    config_file.write_bytes(config)
                    manifest_file.write_bytes(manifest)
                    for source, digest in ((blob, blob_digest), (config_file, "sha256:" + hashlib.sha256(config).hexdigest())):
                        opened = curl(registry + "/v2/" + repository + "/blobs/uploads/", directory,
                                      method="POST", timeout=remaining(), headers=("Content-Length: 0",))
                        opened["body"].unlink()
                        location = upload_location(registry, repository, opened["headers"].get("location", ""))
                        upload_urls.append(location)
                        if source == blob:
                            event("blob_upload_planned", digest=digest, blob_bytes=blob_size)
                        uploaded = curl(upload_location(registry, repository, location, digest), directory,
                                        method="PUT", source=source, rate=rate, timeout=remaining(),
                                        headers=("Content-Type: application/octet-stream",))
                        uploaded["body"].unlink()
                        upload_urls.remove(location)
                        if source == blob:
                            event("blob_committed", digest=digest, blob_bytes=blob_size)
                    manifest_digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
                    event("manifest_planned", digest=manifest_digest, blob_digest=blob_digest, blob_bytes=blob_size)
                    published = curl(registry + "/v2/" + repository + "/manifests/probe-" + str(index), directory,
                                     method="PUT", source=manifest_file, timeout=remaining(),
                                     headers=("Content-Type: " + OCI_MANIFEST,))
                    published["body"].unlink()
                    committed = True
                    readback = curl(registry + "/v2/" + repository + "/blobs/" + blob_digest, directory,
                                    limit=blob_size, rate=rate, timeout=remaining())
                    if readback["body"].stat().st_size != blob_size or digest_file(readback["body"]) != blob_digest:
                        raise RuntimeError("registry blob readback checksum mismatch")
                    readback["body"].unlink()
                    event("registry_roundtrip", bytes=2 * blob_size, stored_blob_bytes=blob_size,
                          digest=blob_digest, seconds=time.monotonic() - fixture_started)
                except Exception as exc:
                    failure("registry", exc)
                finally:
                    if manifest_digest is not None:
                        # A lost PUT reply may still have committed this exact
                        # unique manifest. Delete by our calculated digest only.
                        try:
                            deleted = curl(registry + "/v2/" + repository + "/manifests/" + manifest_digest,
                                           directory, method="DELETE", timeout=remaining(cleanup=True))
                            deleted["body"].unlink()
                            event("manifest_deleted", digest=manifest_digest)
                        except Exception as exc:
                            if committed or "http=404" not in str(exc):
                                failure("manifest", exc, cleanup=True)
                    for location in upload_urls:
                        try:
                            deleted = curl(location, directory, method="DELETE", timeout=remaining(cleanup=True))
                            deleted["body"].unlink()
                        except Exception as exc:
                            if "http=404" not in str(exc):
                                failure("upload", exc, cleanup=True)
                    for path in directory.iterdir():
                        path.unlink()
                index += args.registry_workers
                if budgets["registry"].stopped.is_set():
                    return

        with ThreadPoolExecutor(max_workers=args.nat_workers + args.registry_workers) as pool:
            futures = [pool.submit(nat_worker, index) for index in range(args.nat_workers)]
            futures += [pool.submit(registry_worker, index) for index in range(args.registry_workers)]
            for future in futures:
                future.result()
        report["elapsed_seconds"] = time.monotonic() - started
        report["reserved_payload_bytes"] = sum(budget.reserved for budget in budgets.values())
        for plane, event_name in (("nat", "nat_download"), ("registry", "registry_roundtrip")):
            rows = [row for row in report["events"] if row["event"] == event_name]
            total = sum(row["bytes"] for row in rows)
            active = max((row["elapsed_seconds"] for row in rows), default=0)
            report[plane] = {"completed_operations": len(rows), "verified_payload_bytes": total,
                             "mean_mibps_over_full_run": total / MIB / report["elapsed_seconds"],
                             "seconds_to_final_completion": active,
                             "mean_mibps_to_final_completion": total / MIB / active if active else 0}
        report["stored_blob_bytes_awaiting_normal_gc"] = sum(row["blob_bytes"] for row in report["events"] if row["event"] == "blob_committed")
        report["maximum_unique_blob_bytes_attempted"] = sum(row["blob_bytes"] for row in report["events"] if row["event"] == "blob_upload_planned")
        report["correct"] = not report["errors"] and not report["cleanup_errors"] and report["nat"]["completed_operations"] > 0 and report["registry"]["completed_operations"] > 0
        event("traffic_finished", correct=report["correct"])
    events.close()
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["correct"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--expected-egress-ip", required=True)
    parser.add_argument("--nat-url", default="https://fsn1-speed.hetzner.com/100MB.bin")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration", default=120, type=float)
    parser.add_argument("--max-gib", default=15, type=float)
    parser.add_argument("--nat-mibps", default=100, type=float)
    parser.add_argument("--registry-mibps", default=150, type=float)
    parser.add_argument("--blob-mib", default=64, type=int)
    parser.add_argument("--nat-workers", default=2, type=int)
    parser.add_argument("--registry-workers", default=2, type=int)
    args = parser.parse_args()
    for name in ("duration", "max_gib", "nat_mibps", "registry_mibps"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            parser.error(name + " must be finite and positive")
    if not 15 <= args.duration <= 120 or args.max_gib > 15 or not 1 <= args.blob_mib <= 256:
        parser.error("duration must be 15–120 seconds, max-gib <= 15, blob-mib 1–256")
    if not 1 <= args.nat_workers <= 8 or not 1 <= args.registry_workers <= 8:
        parser.error("each worker count must be 1–8")
    for name in ("registry", "nat_url"):
        url = urlsplit(getattr(args, name))
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
            parser.error(name + " requires an HTTP(S) URL without credentials, query, or fragment")
    if urlsplit(args.nat_url).scheme != "https":
        parser.error("nat-url must use HTTPS")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
