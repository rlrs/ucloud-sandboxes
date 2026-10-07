"""Summarize a build pilot's results.jsonl: per family outcome, time, bytes; builders.
  summarize.py PILOT_DIR
"""
import collections
import datetime
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
rows = [json.loads(line) for line in (root / "results.jsonl").read_text().splitlines()]
tasks = {task["id"]: task for task in json.loads((root / "tasks.json").read_text())["tasks"]}


def pct(values, q):
    values = sorted(v for v in values if v is not None)
    return None if not values else values[min(len(values) - 1, int(q * len(values)))]


def stats(values, scale=1.0, digits=1):
    values = [v / scale for v in values if v is not None]
    if not values:
        return "-"
    return (f"n={len(values)} p50={pct(values, .5):.{digits}f} p90={pct(values, .9):.{digits}f} "
            f"max={max(values):.{digits}f} mean={sum(values) / len(values):.{digits}f}")


def failure_kind(row):
    text = (row.get("error") or "") + "\n" + (row.get("log_tail") or "")
    for pattern, kind in ((r"regeneration failed", "base regeneration"), (r"not found", "context file missing"),
                          (r"Could not resolve|Temporary failure|timed out|50[234]|Connection reset", "network"),
                          (r"ResolutionImpossible|No matching distribution|conflict", "dependency resolution"),
                          (r"apt|dpkg", "apt"), (r"pip|setup.py|wheel", "python build"),
                          (r"exit code", "recipe step failed")):
        if re.search(pattern, text, re.I):
            return kind
    return "other"


print(f"{len(rows)} builds\n")
for family in ("TMax", "Terminal-Lego", "OpenSWE"):
    group = [row for row in rows if row["family"] == family]
    ok = [row for row in group if row["status"] == "succeeded"]
    print(f"== {family}: {len(ok)}/{len(group)} succeeded")
    timing = lambda row, *path: (lambda value: value)(
        __import__("functools").reduce(lambda acc, key: (acc or {}).get(key), path, row.get("timings")))
    print("  end-to-end s         ", stats([timing(r, "end_to_end_ms") for r in ok], 1000))
    print("  docker build+push s  ", stats([timing(r, "phases", "docker_build_and_push_ms") for r in ok], 1000))
    print("  environment publish s", stats([timing(r, "phases", "immutable_environment_ms") for r in ok], 1000))
    print("  queue wait s         ", stats([timing(r, "queue_wait_ms") for r in ok], 1000))
    print("  base regeneration s  ", stats([r.get("base_wait_s") for r in group if r.get("base_wait_s")]))
    print("  client wall s        ", stats([r["wall_s"] for r in ok]))
    print("  new OCI MB           ", stats([r.get("new_oci_bytes") for r in ok], 1e6, 3))
    # The first build on a foundation also rebuilds the regenerated base's EROFS
    # component; later ones only their own groups.
    seen, first, later = set(), [], []
    for r in sorted(ok, key=lambda r: r.get("finished_at") or ""):
        key = tasks[r["id"]]["foundation_key"]
        (later if key in seen else first).append(timing(r, "environment", "erofs_bytes_built"))
        seen.add(key)
    print("  EROFS MB, first on its foundation", stats(first, 1e6))
    print("  EROFS MB, later on a foundation  ", stats(later, 1e6))
    print("  foundations:", len({tasks[r["id"]]["foundation_key"] for r in group}))
    kinds = collections.Counter(failure_kind(r) for r in group if r["status"] != "succeeded")
    if kinds:
        print("  failures:", dict(kinds))
        for r in [r for r in group if r["status"] != "succeeded"][:4]:
            tail = (r.get("log_tail") or r.get("error") or "").strip().splitlines()
            print("   ", r["id"], r["task"], "|", " / ".join(line[:140] for line in tail[-3:]))
    print()

# Builders: how many, and how many builds each ran at once.
spans = collections.defaultdict(list)
for row in rows:
    if row.get("node") and row.get("started_at") and row.get("finished_at"):
        parse = datetime.datetime.fromisoformat
        spans[row["node"]].append((parse(row["started_at"]), parse(row["finished_at"])))
for node, items in sorted(spans.items()):
    events = sorted([(start, 1) for start, _ in items] + [(end, -1) for _, end in items])
    running = peak = 0
    for _, step in events:
        running += step
        peak = max(peak, running)
    busy = (max(end for _, end in items) - min(start for start, _ in items)).total_seconds()
    print(f"builder {node}: {len(items)} builds, peak {peak} at once, span {busy / 60:.1f} min")
starts = [r["started_at"] for r in rows if r.get("started_at")]
ends = [r["finished_at"] for r in rows if r.get("finished_at")]
if starts and ends:
    span = (datetime.datetime.fromisoformat(max(ends)) - datetime.datetime.fromisoformat(min(starts))).total_seconds()
    done = sum(r["status"] == "succeeded" for r in rows)
    print(f"\nwall span {span / 60:.1f} min, {done} succeeded, {done / (span / 3600):.0f} builds/hour")
