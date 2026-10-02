#!/usr/bin/env python3
"""S13: summarize raw/ into raw/summary.json (run locally on the copied results)."""
import glob
import json
import os
import statistics
import sys

R = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "raw")


def load(p):
    try:
        return json.load(open(p))
    except (OSError, ValueError):
        return None


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


out = {}
seq = []
for f in sorted(glob.glob(f"{R}/seq-*.json")):
    seq += load(f) or []
cells = {}
for r in seq:
    if "error" in r and r["error"]:
        continue
    k = (r["path"], r["cmd"])
    c = cells.setdefault(k, {"cold": [], "warm": [], "attach": [], "mb": [], "events": [], "ev_ms": [], "cpu": [], "per_image_cold": {}})
    c["cold"].append(r["cold"]["wall"])
    c["warm"].append(r["warm"]["wall"])
    c["attach"].append(r["attach_wall"])
    c["cpu"].append(r.get("host_cpu_cold_s"))
    st = r.get("stats_after_cold", {})
    cn = st.get("counters", {})
    c["mb"].append(((cn.get("fetched_bytes_demand", 0) + cn.get("fetched_bytes_prefetch", 0)) if r["path"].startswith("nbd")
                    else cn.get("pack_bytes", 0)) / 1e6)
    if not r["path"].startswith("nbd"):
        c["events"].append(cn.get("events"))
        c["ev_ms"].append(st.get("event_ms", {}).get("p50"))
    c["per_image_cold"].setdefault(r["idx"], []).append(r["cold"]["wall"])
out["seq"] = {f"{p} {cmd}": {"n": len(c["cold"]), "cold_median_s": med(c["cold"]), "warm_median_s": med(c["warm"]),
                             "attach_median_s": med(c["attach"]), "mb_read_median": med(c["mb"]),
                             "host_cpu_cold_median_s": med(c["cpu"]),
                             "events_median": med(c["events"]), "event_ms_p50_median": med(c["ev_ms"]),
                             "per_image_cold_median_s": {i: med(v) for i, v in sorted(c["per_image_cold"].items())}}
              for (p, cmd), c in sorted(cells.items())}
bursts = {}
for f in sorted(glob.glob(f"{R}/burst-*.json")):
    b = load(f)
    if b:
        bursts[os.path.basename(f)[6:-5]] = b["summary"]
out["bursts"] = bursts
trees = {}
for f in sorted(glob.glob(f"{R}/tree*.json")):
    for r in load(f) or []:
        t = trees.setdefault(r["idx"], {"family": r["family"], "entries": r["expected_entries"]})
        for k in ("fan:demand", "nbd:demand"):
            if k in r:
                t.setdefault(k + " read_s", r[k]["read_s"])
                if "refill_events" in r[k]:
                    t.setdefault("fan refill_events", r[k]["refill_events"])
                    t.setdefault("fan refill_read_s", r[k]["refill_read_s"])
                    t.setdefault("fan refill_event_ms", r[k]["refill_event_ms"])
                    t.setdefault("filled_after_full_read", r[k]["filled_after_full_read"])
        if r.get("fan_equals_nbd") is not None:
            t["fan_equals_nbd"] = r["fan_equals_nbd"]
        if "classified_fan_vs_oci" in r:
            t["classified_fan_vs_oci"] = {k: v for k, v in r["classified_fan_vs_oci"].items() if k != "examples_non_owner"}
            t["examples_non_owner"] = r["classified_fan_vs_oci"]["examples_non_owner"][:3]
            t["expected_nonroot_owned"] = r.get("expected_nonroot_owned")
out["tree"] = trees
g4 = {}
for f in sorted(glob.glob(f"{R}/gate4-*.json")):
    d = load(f)
    if d and "summary" in d:
        s = d["summary"]
        s["event_ms_p50_median"] = med([x["event_ms"]["p50"] for x in d["steps"]])
        s["events_median"] = med([x["counters"]["events"] for x in d["steps"]])
        s["wall_s_median"] = med([x["wall_s"] for x in d["steps"]])
        g4[d["mode"]] = s
out["gate4"] = g4
pc = {}
for f in sorted(glob.glob(f"{R}/pagecache-*.json")):
    d = load(f)
    if d:
        pc[d["summary"]["variant"]] = d["summary"]
out["pagecache"] = pc
json.dump(out, open(f"{R}/summary.json", "w"), indent=1)
print(json.dumps(out, indent=1)[:20000])
