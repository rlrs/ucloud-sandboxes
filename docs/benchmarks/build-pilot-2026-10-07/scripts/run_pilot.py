"""Build pilot: build each prepared task context through the gateway, as an
integration would, and record time, outcome, builder and new OCI bytes.
  run_pilot.py PILOT_DIR [--in-flight N]
Run on the gateway with the SDK wheel on PYTHONPATH. Results: PILOT_DIR/results.jsonl
(one line per build, appended as builds finish). No token is printed.
"""
import argparse
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib import request

import ucloud_sandboxes_sdk as sdk

GATEWAY = "http://127.0.0.1:8090"
REGISTRY = "10.36.101.16:5000"
TOKEN_FILE = "/var/lib/ucloud-sandboxes/state/sandbox-api-token"
ACCEPT = ", ".join(("application/vnd.oci.image.manifest.v1+json",
                    "application/vnd.docker.distribution.manifest.v2+json"))
REFERENCE = re.compile(re.escape(REGISTRY) + r"/([a-z0-9._/-]+)(?::[A-Za-z0-9._-]+)?(?:@(sha256:[0-9a-f]{64}))?")

parser = argparse.ArgumentParser()
parser.add_argument("pilot_dir", type=Path)
parser.add_argument("--in-flight", type=int, default=24)
parser.add_argument("--timeout", type=float, default=2400)
args = parser.parse_args()
run_id = time.strftime("%m%d%H%M")
tasks = json.loads((args.pilot_dir / "tasks.json").read_text())["tasks"]
results_path = args.pilot_dir / "results.jsonl"
done = {json.loads(line)["id"] for line in results_path.read_text().splitlines()} if results_path.exists() else set()
lock = threading.Lock()
client = sdk.SandboxClient(GATEWAY, api_token=Path(TOKEN_FILE).read_text().strip(), timeout_seconds=120)


def manifest_layers(repository, reference):
    req = request.Request(f"http://{REGISTRY}/v2/{repository}/manifests/{reference}", headers={"Accept": ACCEPT})
    try:
        with request.urlopen(req, timeout=30) as response:
            return {layer["digest"]: layer["size"] for layer in json.load(response).get("layers", [])}
    except Exception:
        return None


def new_bytes(build):
    """Compressed bytes of the built image's layers that none of its bases hold."""
    tag = build.get("tag") or ""
    match = REFERENCE.match(tag)
    if not match:
        return None, []
    built = manifest_layers(match[1], tag.rsplit(":", 1)[1] if "@" not in tag else match[2])
    if built is None:
        return None, []
    bases, base_layers = [], set()
    for line in (build.get("log_tail") or "").splitlines():
        if "load metadata for" in line or "docker-image://" in line:
            for base in REFERENCE.finditer(line):
                reference = base[2] or (base[0].rsplit(":", 1)[1] if base[0].count(":") > 1 else "latest")
                layers = manifest_layers(base[1], reference)
                bases.append({"reference": base[0], "found": layers is not None})
                base_layers |= set(layers or ())
    return sum(size for digest, size in built.items() if digest not in base_layers), bases


def build(task):
    name = f"pilot-{run_id}-{task['id']}"
    image = sdk.Image.from_dockerfile(name=name, context_path=args.pilot_dir / "contexts" / task["id"])
    started = time.time()
    record = {"id": task["id"], "family": task["family"], "task": task["task"], "name": name}
    try:
        # A released base is regenerated on first use; the gateway answers 503
        # base_regenerating until it is ready, and the client retries.
        while True:
            try:
                submitted = client.submit_image_build(image)
                break
            except sdk.SandboxApiError as exc:
                if exc.status_code != 503 or "base_regenerating" not in str(exc) \
                        or time.time() - started > args.timeout:
                    raise
                record["base_wait_s"] = round(time.time() - started, 1)
                time.sleep(10)
        record["build_id"] = submitted.get("build_id")
        result = client.wait_for_image_build(submitted["build_id"], timeout_seconds=args.timeout)
    except Exception as exc:
        record.update(status="client_error", error=f"{type(exc).__name__}: {str(exc)[:400]}")
        result = None
    record["wall_s"] = round(time.time() - started, 1)
    if result is not None:
        record.update(status=result.get("status"), timings=result.get("timings"),
                      node=(result.get("node") or {}).get("job_id"),
                      queued_at=result.get("queued_at"), started_at=result.get("execution_started_at"),
                      finished_at=result.get("finished_at"),
                      error=(result.get("error") or "")[:400] or None,
                      log_tail=(result.get("log_tail") or "")[-3000:] if result.get("status") != "succeeded" else None)
        if result.get("status") == "succeeded":
            record["new_oci_bytes"], record["bases"] = new_bytes(result)
    with lock:
        with results_path.open("a") as out:
            out.write(json.dumps(record) + "\n")
        print(json.dumps({key: record.get(key) for key in ("id", "status", "wall_s", "node", "new_oci_bytes")}),
              flush=True)


pending = [task for task in tasks if task["id"] not in done]
# Interleave the families so each sees the same builder load.
pending.sort(key=lambda task: (int(task["id"].rsplit("-", 1)[1]) % 100, task["id"]))
print(json.dumps({"run_id": run_id, "pending": len(pending), "in_flight": args.in_flight}), flush=True)
with ThreadPoolExecutor(args.in_flight) as pool:
    list(pool.map(build, pending))
print("pilot done", time.strftime("%FT%TZ", time.gmtime()), flush=True)
