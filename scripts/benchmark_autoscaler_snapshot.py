"""Compare autoscaler snapshots with retained exec history on a local database.

PostgreSQL uses UCLOUD_TEST_POSTGRES_DSN and a unique disposable schema. The
fixture is synthetic; timings describe this snapshot, not gateway throughput.
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import replace
import json
import os
from pathlib import Path
import platform
import statistics
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import ExecRoute, RoutingStore, SandboxRoute


def measure(store, *, include_exec_sessions):
    wall = time.perf_counter()
    cpu = time.process_time()
    snapshot = store.load(include_exec_sessions=include_exec_sessions)
    return snapshot, {
        "wall_seconds": time.perf_counter() - wall,
        "client_cpu_seconds": time.process_time() - cpu,
    }


def run(store, *, routes, exec_counts, iterations):
    sandboxes = []
    transaction = store._transaction if getattr(store, "distributed", False) else nullcontext
    with transaction():
        for index in range(routes):
            worker = index % 3
            sandboxes.append(store.upsert_sandbox(SandboxRoute(
                sandbox_id=f"sandbox-{index:05}", node_id=f"node-{worker}",
                job_id=f"job-{worker}", node_url=f"http://node-{worker}:8090",
                resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                spec={"id": f"sandbox-{index:05}",
                      "image": "registry.example/environment@sha256:" + "a" * 64,
                      "command": ["python", "agent.py"],
                      "environment": {"LANG": "C.UTF-8"}},
                state="running", generation=1,
                create_operation_id=f"create-{index}", spec_hash="b" * 64,
            )))
    results = []
    seeded = 0
    for count in sorted(set(exec_counts)):
        with transaction():
            for index in range(seeded, count):
                route = sandboxes[index % routes]
                store.upsert_exec(ExecRoute(
                    session_id=f"exec-{index:07}", sandbox_id=route.sandbox_id,
                    node_id=route.node_id, job_id=route.job_id, node_url=route.node_url,
                ))
        seeded = count
        full = store.load()
        projected = store.load(include_exec_sessions=False)
        assert len(full.exec_sessions) == count
        assert len(full.sandboxes) == routes
        assert projected == replace(full, exec_sessions={})
        samples = {"full": [], "autoscaler": []}
        for iteration in range(iterations):
            order = ("full", "autoscaler") if iteration % 2 == 0 else ("autoscaler", "full")
            for name in order:
                _snapshot, sample = measure(store, include_exec_sessions=name == "full")
                samples[name].append(sample)
        results.append({
            "routes": routes, "exec_sessions": count,
            "samples": samples,
            "median": {
                name: {key: statistics.median(row[key] for row in rows)
                       for key in rows[0]}
                for name, rows in samples.items()
            },
        })
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "postgres"), default="sqlite")
    parser.add_argument("--routes", type=int, default=500)
    parser.add_argument("--exec-counts", type=int, nargs="+", default=[0, 5000, 10000])
    parser.add_argument("--iterations", type=int, default=25)
    args = parser.parse_args()
    if args.routes < 1 or args.iterations < 1 or min(args.exec_counts) < 0:
        parser.error("routes and iterations must be positive, exec counts nonnegative")
    with TemporaryDirectory(prefix="ucloud-autoscaler-benchmark-") as raw:
        path = Path(raw) / "routes.sqlite"
        store = None
        schema = None
        postgres_version = None
        try:
            if args.backend == "postgres":
                import psycopg
                from psycopg import sql
                from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore
                dsn = os.environ["UCLOUD_TEST_POSTGRES_DSN"]
                schema = "ucloud_routing_benchmark_" + uuid4().hex
                store = PostgresRoutingStore(path, dsn=dsn, schema=schema)
                store.migrate()
                with psycopg.connect(dsn) as conn:
                    postgres_version = conn.execute("SHOW server_version").fetchone()[0]
            else:
                store = RoutingStore(path)
            result = {
                "backend": args.backend, "python": platform.python_version(),
                "platform": platform.platform(), "postgres_version": postgres_version,
                "iterations": args.iterations,
                "timing_scope": "snapshot wall time and Python client CPU; excludes PostgreSQL server CPU",
                "results": run(store, routes=args.routes, exec_counts=args.exec_counts,
                               iterations=args.iterations),
            }
            print(json.dumps(result, indent=2))
        finally:
            if schema is not None:
                if store is not None:
                    store.close()
                with psycopg.connect(dsn, autocommit=True) as conn:
                    conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()
