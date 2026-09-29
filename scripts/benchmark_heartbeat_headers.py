"""Compare exact durable heartbeat header reads with a specified Git baseline.

Uses temporary synthetic SQLite state and never contacts production. Both
versions read identical three-node inventories; the only substituted method is
the baseline get_heartbeat implementation. This is a component CPU benchmark,
not a complete gateway capacity qualification.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import replace
import json
from pathlib import Path
import platform
import statistics
import subprocess
from tempfile import TemporaryDirectory
import time

from ucloud_sandboxes import control_state
from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry


def baseline_method(revision):
    source = subprocess.check_output(
        ["git", "show", f"{revision}:ucloud_sandboxes/control_state.py"], text=True,
    )
    cls = next(node for node in ast.parse(source).body
               if isinstance(node, ast.ClassDef) and node.name == "ControlStateStore")
    method = next(node for node in cls.body
                  if isinstance(node, ast.FunctionDef) and node.name == "get_heartbeat")
    namespace = vars(control_state).copy()
    exec(compile(ast.Module(body=[method], type_ignores=[]), "<baseline>", "exec"), namespace)
    return namespace["get_heartbeat"]


def benchmark(store, before, *, nodes, inventory_count, reads, repeats):
    for node in range(nodes):
        heartbeat = build_heartbeat(
            job_id=f"job-{node}", node_id=f"node-{node}", deployment_id="benchmark",
            node_url=f"http://worker-{node}:8090",
        )
        inventory = tuple(SandboxInventoryEntry(
            sandbox_id=f"sandbox-{node}-{index}", generation=1, operation_id="create",
            spec_hash="a" * 64, state="running",
            resources=ResourceQuantity(1, 1024, 5184),
            storage_dependency={"layers": [
                {"digest": "sha256:" + "b" * 64, "size": 1024} for _ in range(4)
            ]},
        ) for index in range(inventory_count))
        store.upsert_heartbeat(replace(heartbeat, inventory=inventory, inventory_complete=True))
    methods = [("before", before), ("after", control_state.ControlStateStore.get_heartbeat)]
    for _name, method in methods:
        for node in range(nodes):
            method(store, f"job-{node}", include_inventory=False)
    samples = {name: [] for name, _method in methods}
    for repeat in range(repeats):
        for name, method in methods[::(-1 if repeat % 2 else 1)]:
            cpu, wall = time.process_time(), time.perf_counter()
            for index in range(reads):
                method(store, f"job-{index % nodes}", include_inventory=False)
            samples[name].append({
                "cpu_us_per_read": (time.process_time() - cpu) * 1e6 / reads,
                "wall_us_per_read": (time.perf_counter() - wall) * 1e6 / reads,
            })
    with store._connection() as connection:
        payload_bytes = connection.execute(
            "SELECT sum(length(payload)) FROM control_records WHERE namespace='heartbeat'",
        ).fetchone()[0]
    medians = {name: statistics.median(row["cpu_us_per_read"] for row in rows)
               for name, rows in samples.items()}
    return {
        "nodes": nodes, "inventory_per_node": inventory_count,
        "total_heartbeat_payload_bytes": payload_bytes,
        "median_cpu_us_per_header_read": medians,
        "cpu_reduction_percent": 100 * (1 - medians["after"] / medians["before"]),
        "samples": samples,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--inventories", type=int, nargs="+", default=[0, 170, 512])
    parser.add_argument("--reads", type=int, default=6000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.nodes, args.reads, args.repeat) < 1 or min(args.inventories) < 0:
        parser.error("nodes, reads, and repeat must be positive; inventories must be nonnegative")
    revision = subprocess.check_output(["git", "rev-parse", args.baseline], text=True).strip()
    before = baseline_method(revision)
    with TemporaryDirectory() as directory:
        store = control_state.ControlStateStore(Path(directory) / "control.sqlite")
        results = [benchmark(store, before, nodes=args.nodes, inventory_count=count,
                             reads=args.reads, repeats=args.repeat)
                   for count in args.inventories]
    report = {"baseline": revision, "python": platform.python_version(),
              "platform": platform.platform(), "reads_per_sample": args.reads,
              "repeat": args.repeat, "results": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
