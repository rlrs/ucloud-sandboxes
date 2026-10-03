#!/usr/bin/env python3
"""Why pause reclaim frees ~13 MB: one paused sandbox per reclaim variant.

Run as root on an idle pause-tier worker (docs/benchmarks/pause-reclaim-2026-10-03).
Each sandbox is a managed agent with a random heap (benchmark_sandbox_density's
WORKLOAD), worked once, then paused through a relay-shaped park. The variant
writes memory.reclaim to the sandbox's own cgroup directly; afterwards a relay
wake and one unit of work time the way back.

  A  production loop: 16 MiB windows, stop when a window frees < 1 MiB (zswap on)
  B  one write for all of memory.current (zswap on)
  C  one write, zswap off for this cgroup (memory.zswap.max=0)
  E  production loop, zswap off for this cgroup
"""
import argparse
import errno
import glob
import hashlib
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import benchmark_sandbox_density as bench

WINDOW = 16 * 1024 ** 2
VARIANTS = {"A": (True, False), "B": (False, False), "C": (False, True), "E": (True, True)}


def cgroup_of(sandbox_id, generation=1):
    """cgroupsPath is /ucloud-sandboxes/<sha256(id:generation)> (image_rootfs.py)."""
    path = Path("/sys/fs/cgroup/ucloud-sandboxes") / hashlib.sha256(f"{sandbox_id}:{generation}".encode()).hexdigest()
    if not (path / "memory.reclaim").exists():
        raise RuntimeError(f"no cgroup for {sandbox_id} at {path}: {glob.glob('/sys/fs/cgroup/ucloud-sandboxes/*')[:3]}")
    return path


def stat(cgroup):
    values = dict(line.split() for line in (cgroup / "memory.stat").read_text().splitlines())
    return {"current": int((cgroup / "memory.current").read_text()),
            "swap": int((cgroup / "memory.swap.current").read_text()),
            "zswap": int(values.get("zswap", 0)), "zswapped": int(values.get("zswapped", 0)),
            "shmem": int(values.get("shmem", 0)), "anon": int(values.get("anon", 0)),
            "file": int(values.get("file", 0))}


def reclaim(cgroup, *, windowed):
    """(writes, reason): the production loop's stop rule, or one write."""
    fd = os.open(cgroup / "memory.reclaim", os.O_WRONLY)
    writes, reason = 0, "done"
    try:
        if not windowed:
            try:
                os.write(fd, f"{stat(cgroup)['current']} swappiness=200".encode())
            except OSError as exc:
                reason = "eagain" if exc.errno == errno.EAGAIN else f"errno {exc.errno}"
            return 1, reason
        while True:
            before = stat(cgroup)["current"]
            try:
                os.write(fd, f"{WINDOW} swappiness=200".encode())
            except OSError as exc:
                if exc.errno != errno.EAGAIN:
                    return writes, f"errno {exc.errno}"
            writes += 1
            if before - stat(cgroup)["current"] < WINDOW // 16:
                return writes, "not_shrinking"
    finally:
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--image", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--resident-mb", type=int, default=1536)
    args = parser.parse_args()
    env = dict(line.split("=", 1) for line in Path("/etc/ucloud-sandboxes/node.env").read_text().splitlines()
               if "=" in line and not line.startswith("#"))
    token = Path(env["UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE"].strip("'\"")).read_text().strip()
    api = bench.NodeApi(f"http://127.0.0.1:{env['UCLOUD_NODE_AGENT_PORT'].strip(chr(39))}", token)
    spec = argparse.Namespace(image=args.image, cpus=1.0, memory_mb=2048, disk_mb=4096,
                              resident_mb=args.resident_mb, dirty_mb=384, dirty_page_count=384 * 256)
    prefix = "reclaim-" + uuid4().hex[:8]
    api.call("/v1/images/pull", method="POST", payload={"image": args.image, "id": prefix})
    results, owned = {}, []
    try:
        for variant, (windowed, no_zswap) in VARIANTS.items():
            sandbox_id = f"{prefix}-{variant}"
            owned.append(sandbox_id)
            record, state = bench.create_resident(api, spec, sandbox_id, managed=True)
            state = api.probe(sandbox_id, act=True)  # Touch the heap: it is hot, like a live agent's.
            relay_id = uuid4().hex
            park = api.call(f"/v1/sandboxes/{sandbox_id}/park", method="POST", payload={
                "operation_id": "park:" + relay_id, "relay_request_id": relay_id, "generation": record["generation"]})
            cgroup = cgroup_of(sandbox_id, record["generation"])
            if no_zswap:
                (cgroup / "memory.zswap.max").write_text("0")
            before = stat(cgroup)
            started = time.monotonic()
            writes, reason = reclaim(cgroup, windowed=windowed)
            seconds = time.monotonic() - started
            after = stat(cgroup)
            time.sleep(5)
            begin = time.monotonic()
            api.call(f"/v1/sandboxes/{sandbox_id}/wake", method="POST", payload={
                "operation_id": "wake:" + relay_id, "generation": record["generation"], "relay_request_id": relay_id})
            wake_ms = (time.monotonic() - begin) * 1000
            begin = time.monotonic()
            later = api.probe(sandbox_id, act=True)
            act_ms = (time.monotonic() - begin) * 1000
            bench.check_identity(state, later, advanced=True)  # Same process, heap intact.
            results[variant] = {
                "windowed": windowed, "zswap_off": no_zswap, "park": park["sandbox"].get("state"),
                "writes": writes, "reason": reason, "seconds": round(seconds, 2),
                "freed_mb": round((before["current"] - after["current"]) / 2 ** 20, 1),
                "swap_mb": round((after["swap"] - before["swap"]) / 2 ** 20, 1),
                "zswapped_mb": round(after["zswapped"] / 2 ** 20, 1), "zswap_pool_mb": round(after["zswap"] / 2 ** 20, 1),
                "rate_mb_s": round((before["current"] - after["current"]) / 2 ** 20 / max(seconds, 1e-3), 1),
                "wake_ms": round(wake_ms, 1), "act_after_ms": round(act_ms, 1),
                "first_act_ms": round(state.get("guest_timings_ms", {}).get("total", 0), 1),
                "before": before, "after": after}
            print(variant, json.dumps({key: value for key, value in results[variant].items()
                                       if key not in ("before", "after")}), flush=True)
            Path(args.out).write_text(json.dumps(results, indent=1))
    finally:
        for sandbox_id in owned:
            try:
                api.call(f"/v1/sandboxes/{sandbox_id}", method="DELETE", headers={
                    "X-UCloud-Sandbox-Generation": "1", "X-UCloud-Sandbox-Operation-Id": "delete-" + sandbox_id})
            except Exception as exc:  # noqa: BLE001
                print("delete", sandbox_id, exc)


if __name__ == "__main__":
    main()
