#!/usr/bin/env python3
"""Build sampled OpenSWE task images locally with docker from the registry-hosted foundations.

FROM lines are digest-pinned to 10.42.0.2:5000 (read-only pulls). Images are pushed only to the
VM-local registry at localhost:5001. No Docker Hub access is needed: every FROM is checked first.
"""
import json, os, re, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
tasks = json.load(open(os.environ.get("TASKS", "/root/openswe-tasks.json")))
out = open("/data/logs/openswe-builds.jsonl", "a")

def build(t):
    froms = re.findall(r"^\s*FROM\s+(\S+)", t["dockerfile"], re.M | re.I)
    if not froms or any(not f.startswith("10.42.0.2:5000/") or "@sha256:" not in f for f in froms):
        return {"task": t["task"], "status": "skipped_non_registry_from", "froms": froms}
    d = f"/data/openswe/{t['task']}"
    os.makedirs(d, exist_ok=True)
    open(f"{d}/Dockerfile", "w").write(t["dockerfile"])
    tag = "localhost:5001/openswe/" + t["task"].lower().replace("__", ".").replace("openswe--", "")
    t0 = time.time()
    p = subprocess.run(["docker", "build", "--network", "host", "-t", tag, d], capture_output=True, text=True,
                       env=dict(os.environ, DOCKER_BUILDKIT="0"))
    rec = {"task": t["task"], "tag": tag, "build_seconds": round(time.time() - t0, 1), "rc": p.returncode}
    if p.returncode != 0:
        rec["status"] = "build_failed"; rec["tail"] = (p.stdout + p.stderr)[-1500:]
        return rec
    t1 = time.time()
    q = subprocess.run(["docker", "push", tag], capture_output=True, text=True)
    rec.update(push_seconds=round(time.time() - t1, 1), push_rc=q.returncode,
               status="ok" if q.returncode == 0 else "push_failed")
    if q.returncode:
        rec["tail"] = (q.stdout + q.stderr)[-800:]
    return rec

with ThreadPoolExecutor(int(sys.argv[1]) if len(sys.argv) > 1 else 4) as ex:
    for r in ex.map(build, tasks):
        out.write(json.dumps(r) + "\n"); out.flush()
        print(r["task"], r["status"], r.get("build_seconds"), flush=True)
