#!/usr/bin/env python3
import json, os, subprocess, sys, time
n = int(sys.argv[1]) if len(sys.argv) > 1 else 40
rows = []
for l in open("/data/w/log/pipeline.jsonl"):
    r = json.loads(l)
    if r["step"] in ("build", "eval", "verify") or (r["step"] in ("slim", "walk") and r["rc"] != 0):
        rows.append(f'{r["step"]:6} {r.get("task","")[9:45]:36} {r.get("variant",""):16} rc={r["rc"]} {r["s"]}s '
                    + (r.get("out") or r.get("tail") or "")[-110:].replace("\n", " | "))
print("\n".join(rows[-n:]))
print(time.strftime("%T"), "trees", len(os.listdir("/data/w/trees")), "load", open("/proc/loadavg").read().split()[:3])
print(subprocess.run("pgrep -af '^docker (build|run|export)' | cut -c1-110", shell=True, capture_output=True, text=True).stdout)
