"""Exec latency probe: sequential and concurrent execs, start vs wait split.

usage: exec_probe.py <gateway-url> <token-file> <create|reuse|delete> <n> [concurrency]
Sandbox ids are exec-probe-<i>.
"""
import json, statistics, sys, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

for candidate in ("/Users/Rasmus/Git/ucloud-sandboxes/ucloud-sandboxes-sdk/src", "/root/sdk"):
    if Path(candidate).is_dir():
        sys.path.insert(0, candidate)
from ucloud_sandboxes_sdk.client import Image, SandboxClient, SandboxSpec

url, token_file, mode, n = sys.argv[1], Path(sys.argv[2]), sys.argv[3], int(sys.argv[4])
concurrency = int(sys.argv[5]) if len(sys.argv) > 5 else n
client = SandboxClient(url, timeout_seconds=300, api_token=token_file.read_text().strip())
ids = [f"exec-probe-{i}" for i in range(n)]
image = Image.from_registry("python@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285")

if mode == "create":
    with ThreadPoolExecutor(max_workers=min(n, 32)) as pool:
        list(pool.map(lambda sid: client.create_sandbox(SandboxSpec(
            id=sid, image=image, memory_mb=512, cpus=1, disk_mb=1024,
            command=("sleep", "infinity"))), ids))
    print("created", n)
    sys.exit(0)
if mode == "delete":
    with ThreadPoolExecutor(max_workers=min(n, 32)) as pool:
        list(pool.map(lambda sid: client.delete_sandbox(sid), ids))
    print("deleted", n)
    sys.exit(0)


def one(sid):
    t0 = time.monotonic()
    handle = client.start_exec(sid, ["sh", "-c", "printf ready"])
    t1 = time.monotonic()
    result = handle.wait(timeout_seconds=120)
    t2 = time.monotonic()
    return (t1 - t0) * 1000, (t2 - t1) * 1000, result.exit_code


def summary(rows):
    def q(values, p):
        values = sorted(values)
        return round(values[min(len(values) - 1, int(len(values) * p))])
    starts, waits = [r[0] for r in rows], [r[1] for r in rows]
    total = [r[0] + r[1] for r in rows]
    return {"n": len(rows), "start_p50": q(starts, .5), "start_p95": q(starts, .95),
            "wait_p50": q(waits, .5), "wait_p95": q(waits, .95), "total_p50": q(total, .5),
            "total_p95": q(total, .95), "failed": sum(1 for r in rows if r[2] != 0)}


sequential = [one(ids[i % n]) for i in range(10)]
with ThreadPoolExecutor(max_workers=concurrency) as pool:
    started = time.monotonic()
    concurrent = list(pool.map(one, [ids[i % n] for i in range(concurrency)]))
    wall = time.monotonic() - started
print(json.dumps({"url": url, "sequential": summary(sequential),
                  "concurrent": {**summary(concurrent), "wall_s": round(wall, 2), "concurrency": concurrency}}))
