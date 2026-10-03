"""C9.1-style probe: raw 256 KiB ranged reads from the gateway registry, no NBD or cache.

Pools: 'warm' = components of the spike's 64 images (just read), 'cold' = components of
64 other production images. Prints MB/s and latency per concurrency.
"""
import json
import random
import re
import sys
import threading
import time
import urllib.request
from pathlib import Path

wrapper = Path("/work/ucloud-sandboxes/bin/ucloud-sandboxes").read_text()
sys.path.insert(0, re.search(r"PYTHONPATH=(\S+)", wrapper).group(1))
from ucloud_sandboxes.environment_artifact import load_image_environment  # noqa: E402
from ucloud_sandboxes.environment_config import environment_registry_from_args  # noqa: E402
from ucloud_sandboxes.managed_registry import RegistryClient, manifest_digest_from_image_ref  # noqa: E402
from ucloud_sandboxes.managed_registry import registry_repository_tag_from_image_ref  # noqa: E402

import os
CHUNK = int(os.environ.get("PROBE_RANGE", 256 * 1024))
args = type("A", (), {"environment_registry_url": "http://10.42.0.2:5000", "environment_registry_repository": "environments",
                      "environment_trusted_keys": "/etc/ucloud-sandboxes/environment/producers.json"})()
registry = environment_registry_from_args(args)
client = RegistryClient("http://10.42.0.2:5000")


def blobs(refs):
    found = {}
    for ref in refs:
        repository, _ = registry_repository_tag_from_image_ref(ref)
        _, environment = load_image_environment(registry, repository, manifest_digest_from_image_ref(ref))
        for digest in environment.components:
            component = registry.load(digest)
            request = urllib.request.Request(f"http://10.42.0.2:5000/v2/environments/blobs/{component.image_digest}",
                                             method="HEAD")
            with urllib.request.urlopen(request, timeout=30) as response:
                found[component.image_digest] = int(response.headers["Content-Length"])
    return list(found.items())


def run(pool, concurrency, seconds=15):
    stop, done, lat, lock, errors = time.monotonic() + seconds, [0], [], threading.Lock(), [0]

    def worker(seed):
        rng = random.Random(seed)
        while time.monotonic() < stop:
            digest, size = rng.choice(pool)
            offset = rng.randrange(0, max(1, size // CHUNK - 1)) * CHUNK
            began = time.monotonic()
            try:
                data = client.blob_range("environments", digest, offset, CHUNK, timeout_seconds=60)
            except Exception:  # noqa: BLE001 - counted, not fatal
                with lock:
                    errors[0] += 1
                continue
            with lock:
                done[0] += len(data)
                lat.append(time.monotonic() - began)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    began = time.monotonic()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - began
    lat.sort()
    return {"concurrency": concurrency, "MBps": round(done[0] / elapsed / 1e6, 1), "requests": len(lat),
            "p50_ms": round(lat[len(lat) // 2] * 1000, 1), "p95_ms": round(lat[int(len(lat) * .95)] * 1000, 1), "errors": errors[0]}


warm_refs = list(json.load(open("/opt/attach-spike/bench-64.json")).values())
cold_refs = [line.strip() for line in open("/opt/attach-spike/cold-64.txt") if line.strip()]
for name, refs in ((("warm", warm_refs),) if os.environ.get("PROBE_WARM_ONLY") else (("warm", warm_refs), ("cold", cold_refs))):
    pool = blobs(refs)
    print(name, "blobs", len(pool), "GB", round(sum(s for _, s in pool) / 1e9, 1), flush=True)
    for concurrency in tuple(int(c) for c in os.environ.get("PROBE_CONCURRENCY", "8,32,64").split(",")):
        print(name, json.dumps(run(pool, concurrency)), flush=True)
