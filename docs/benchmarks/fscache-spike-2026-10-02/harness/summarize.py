"""Summaries for the README: medians per family and path, parallel runs, storage."""
import json
from pathlib import Path
import statistics as st
import sys

OUT = Path(sys.argv[1] if len(sys.argv) > 1 else "/root/s11/out")


def med(xs):
    xs = [x for x in xs if x is not None]
    return round(st.median(xs), 3) if xs else None


def load(name):
    p = OUT / name
    return json.loads(p.read_text()) if p.exists() else None


summary = {"seq": {}, "par": {}}
for path in ("fscache", "nbd"):
    d = load(f"bench-{path}-seq.json")
    if not d:
        continue
    by = {}
    for r in d["images"]:
        fam = "Python images (ScaleSWE, TMax 1-5, T-Lego 0/2/4)" if r["cold"]["python_import_sys"]["rc"] == 0 else "no python3 (TMax 0, T-Lego 1/3/5)"
        by.setdefault(fam, []).append(r)
        by.setdefault("all", []).append(r)
    out = {}
    for fam, rs in by.items():
        py = [r for r in rs if r["cold"]["python_import_sys"]["rc"] == 0]
        out[fam] = {
            "n": len(rs),
            "attach_s": med(r["attach"]["attach_s"] for r in rs),
            "ready_s (attach+overlay+runsc start)": med(r["ready_s"] for r in rs),
            "cold": {k: med(r["cold"][k]["seconds"] for r in py if r["cold"][k]["rc"] == 0)
                     for k in ("python_import_sys", "git_status", "pip_version", "pytest_import")},
            "warm": {k: med(r["warm"][k]["seconds"] for r in py if r["warm"][k]["rc"] == 0)
                     for k in ("python_import_sys", "git_status", "pip_version", "pytest_import")},
            "bytes_fetched": med(r["host"]["bytes_fetched"] for r in rs),
            "bytes_fetched_sum": sum(r["host"]["bytes_fetched"] for r in rs),
            "cache_allocated_bytes": med(r["cache_usage_bytes"][0] for r in rs),
            "cache_allocated_sum": sum(r["cache_usage_bytes"][0] for r in rs),
            "daemon_cpu_s": med(r["host"]["daemon_cpu_s"] for r in rs),
            "host_busy_cpu_s": med(r["host"]["host_busy_cpu_s"] for r in rs),
        }
    summary["seq"][path] = out
    summary["seq"][path]["per_image"] = {
        r["image"]: {"attach_s": round(r["attach"]["attach_s"], 3), "ready_s": round(r["ready_s"], 3),
                     "cold": {k: [round(v["seconds"], 3), v["rc"]] for k, v in r["cold"].items()},
                     "warm": {k: round(v["seconds"], 3) for k, v in r["warm"].items()},
                     "bytes": r["host"]["bytes_fetched"], "cache": r["cache_usage_bytes"][0]}
        for r in d["images"]}
for path in ("fscache", "nbd"):
    runs = []
    for name in [f"bench-{path}-par-r{i}.json" for i in (1, 2, 3, 4)] + [f"bench-{path}-par-x3.json"]:
        d = load(name)
        if not d:
            continue
        ok = [r for r in d["images"] if "error" not in r]
        py = [r for r in ok if r["cold"]["python_import_sys"]["rc"] == 0]
        runs.append({
            "file": name, "sandboxes": len(d["images"]), "ok": len(ok),
            "errors": sorted({r["error"][:160] for r in d["images"] if "error" in r}),
            "wall_s": round(d["host"]["wall_s"], 2),
            "host_busy_cpu_s": round(d["host"]["host_busy_cpu_s"], 1),
            "host_cpu_util": round(d["host"]["host_cpu_util"], 3),
            "peak_cpu_util_0.5s": round(d["host"]["peak_cpu_util_0.5s"], 3),
            "daemon_cpu_s": round(d["host"]["daemon_cpu_s"], 2),
            "bytes_fetched": d["host"]["bytes_fetched"],
            "cache_allocated_bytes": d["cache_usage_bytes"][0],
            "attach_s_median": med(r["attach"]["attach_s"] for r in ok),
            "attach_s_max": round(max(r["attach"]["attach_s"] for r in ok), 3) if ok else None,
            "first_command_done_s_median": med(r["first_command_done_s"] for r in py),
            "first_command_done_s_max": round(max(r["first_command_done_s"] for r in py), 3) if py else None,
            "pip_version_cold_median": med(r["cold"]["pip_version"]["seconds"] for r in py),
            "all_cold_done_s_max": round(max(r["all_cold_done_s"] for r in ok), 3) if ok else None,
            "daemon_metrics": d.get("daemon_metrics") if path == "nbd" else None,
        })
    summary["par"][path] = runs
for extra in ("storage.json", "verify-fscache.json", "backend-client-eagain.json"):
    d = load(extra)
    if d is not None:
        summary[extra.split(".")[0]] = d.get("totals_20", d) if extra == "storage.json" else (
            {k: v for k, v in d.items() if k != "contents_vs_oci"} if extra == "verify-fscache.json" else d)
v = load("verify-fscache.json")
if v and "contents_vs_oci" in v:
    c = v["contents_vs_oci"]
    summary["contents_vs_oci"] = {"images": len(c), "sampled_files": sum(x["sampled"] for x in c.values()),
                                  "matched": sum(x["matched"] for x in c.values())}
(OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str))
print(json.dumps({k: summary[k] for k in ("seq",)}, indent=1, default=str)[:6000])
print(json.dumps(summary["par"], indent=1, default=str)[:6000])
