"""Synthetic complete fleet read CPU/bytes: full vs opt-in compact status.

Both modes observe the same durable routes and complete worker inventories.
Unchanged and heartbeat-invalidated encodings are measured separately. These
are local process CPU measurements, not end-to-end production capacity gates.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
import platform
import statistics
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.control_plane import _sandbox_list_bytes
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.fleet_reader import FleetResponseRenderer
from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry, utc_now
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute


def benchmark(root, *, count, nodes, reads, repeats, postgres=False):
    control = ControlStateStore(root / "control.sqlite")
    schema = None
    if postgres:
        from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore
        schema = "ucloud_routing_fleet_bench_" + uuid4().hex
        routes = PostgresRoutingStore(root / "routes.sqlite",
            dsn=os.environ["UCLOUD_TEST_POSTGRES_DSN"], schema=schema)
        routes.migrate()
    else:
        routes = RoutingStore(root / "routes.sqlite")
    try:
        inventory = [[] for _ in range(nodes)]
        resources = ResourceQuantity(1, 1024, 4096)
        for index in range(count):
            node = index % nodes
            sid = f"sandbox-{index:04}"
            layers = [{"digest": "sha256:" + str(layer) * 64, "size": 32 * 1024**2}
                      for layer in range(4)]
            inventory[node].append(SandboxInventoryEntry(
                sandbox_id=sid, state="running", generation=1, operation_id="create",
                spec_hash="a" * 64, resources=resources, storage_schema="storage-native-v1",
                snapshot_manifest_digest="sha256:" + "b" * 64,
                snapshot_repository="benchmark/snapshots", snapshot_tag=sid,
                storage_snapshot={"version": 1, "layers": layers},
                storage_dependency={"layers": layers},
            ))
            routes.upsert_sandbox(SandboxRoute(
                sandbox_id=sid, node_id=f"node-{node}", job_id=f"job-{node}",
                node_url=f"http://worker-{node}:8090", resources=resources,
                state="running", generation=1, create_operation_id="create", spec_hash="a" * 64,
                spec={"id": sid, "image": "registry.test/python@sha256:" + "c" * 64,
                      "cpus": 1, "memory_mb": 1024, "disk_mb": 4096, "parkable": True,
                      "managed_process": True,
                      "command": ["python", "-c", "# representative managed agent program\n" * 12],
                      "labels": {"owner": "benchmark", "purpose": "fleet-status"}},
                storage_snapshot={"version": 1, "layers": layers},
            ))
        heartbeats = [replace(build_heartbeat(
            job_id=f"job-{node}", node_id=f"node-{node}", deployment_id="benchmark",
            node_url=f"http://worker-{node}:8090"), active_sandboxes=len(entries),
            inventory=tuple(entries), inventory_complete=True)
            for node, entries in enumerate(inventory)]
        for heartbeat in heartbeats:
            control.upsert_heartbeat(heartbeat)
        modes = {"full": (False, ()), "status": (True, ()),
                 "status_16_ids": (True, tuple(f"sandbox-{i:04}" for i in range(min(16, count))))}
        renderers = {name: FleetResponseRenderer(status_only=status)
                     for name, (status, _ids) in modes.items()}

        def render(name):
            status, ids = modes[name]
            return _sandbox_list_bytes(control, routes, 3600, renderer=renderers[name],
                                       status_only=status, sandbox_ids=ids)

        initial = {name: render(name) for name in modes}
        full = json.loads(initial["full"])["sandboxes"]
        projected = [{**{key: row[key] for key in ("id", "state", "cached_state", "node",
                                                   "created_at", "updated_at")},
                      "spec": {"id": row["id"]}, "generation": 1} for row in full]
        assert json.loads(initial["status"])["sandboxes"] == projected
        assert json.loads(initial["status_16_ids"])["sandboxes"] == projected[:16]
        results = {}
        for refresh in (False, True):
            samples = {name: [] for name in modes}
            for repeat in range(repeats):
                for name in list(modes)[::(-1 if repeat % 2 else 1)]:
                    cpu_total = wall_total = 0
                    for _ in range(reads):
                        if refresh:
                            for heartbeat in heartbeats:
                                control.upsert_heartbeat(replace(heartbeat, updated_at=utc_now()))
                        cpu, wall = time.process_time(), time.perf_counter()
                        result = render(name)
                        cpu_total += time.process_time() - cpu
                        wall_total += time.perf_counter() - wall
                        assert result == initial[name]
                    samples[name].append({"cpu_ms_per_read": cpu_total * 1000 / reads,
                                          "wall_ms_per_read": wall_total * 1000 / reads})
            medians = {name: statistics.median(s["cpu_ms_per_read"] for s in values)
                       for name, values in samples.items()}
            results["fresh_heartbeat_each_read" if refresh else "unchanged"] = {
                "median_cpu_ms_per_read": medians,
                "status_cpu_reduction_percent": 100 * (1 - medians["status"] / medians["full"]),
                "samples": samples,
            }
        return {"routes": count, "nodes": nodes, "backend": "postgres" if postgres else "sqlite",
                "response_bytes": {name: len(body) for name, body in initial.items()},
                "equivalent_visible_status": True, "measurements": results}
    finally:
        if schema:
            import psycopg
            from psycopg import sql
            routes.close()
            with psycopg.connect(os.environ["UCLOUD_TEST_POSTGRES_DSN"], autocommit=True) as db:
                db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sandboxes", type=int, default=500)
    parser.add_argument("--nodes", type=int, default=2)
    parser.add_argument("--reads", type=int, default=30)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--postgres", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.sandboxes, args.nodes, args.reads, args.repeat) < 1:
        parser.error("counts must be positive")
    with TemporaryDirectory() as raw:
        result = benchmark(Path(raw), count=args.sandboxes, nodes=args.nodes,
                           reads=args.reads, repeats=args.repeat, postgres=args.postgres)
    report = {"python": platform.python_version(), "platform": platform.platform(),
              "reads_per_sample": args.reads, "repeats": args.repeat,
              "scope": "synthetic complete read/render CPU; excludes IPC, HTTP, TLS and client parse; not production capacity",
              **result}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "measurements"}))
    for name, result in report["measurements"].items():
        print(name, json.dumps({k: v for k, v in result.items() if k != "samples"}))


if __name__ == "__main__":
    main()
