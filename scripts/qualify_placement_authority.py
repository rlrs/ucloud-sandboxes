"""Disposable PostgreSQL routing concurrency qualification, not a wake benchmark.

Uses the production repository and transactional reservation boundary. Run on
Linux with UCLOUD_TEST_POSTGRES_DSN set to a qualification database.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import time
from uuid import uuid4

import psycopg
from psycopg import sql

from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import SandboxRouteAllocation
from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore


def qualify(count, nodes):
    dsn = os.environ["UCLOUD_TEST_POSTGRES_DSN"]
    schema = "ucloud_routing_load_" + uuid4().hex
    store = PostgresRoutingStore(
        Path("/tmp/" + schema), dsn=dsn, schema=schema, max_connections=32
    )
    store.migrate()
    slots = math.ceil(count / nodes)
    timings = []

    def reserve(i):
        start = time.monotonic()

        def transaction():
            job = "worker-" + str(i % nodes)
            residents = store._sandbox_route_rows_readonly(
                node_identity=(job, job, "http://" + job, "http://" + job + "/")
            )
            if len(residents) >= slots:
                return None
            return store.allocate_sandbox_create_with_pending(
                SandboxRouteAllocation(
                    sandbox_id="load-" + str(i),
                    node_id=job,
                    job_id=job,
                    node_url="http://" + job,
                    resources=ResourceQuantity(memory_mb=1024),
                    spec={"id": "load-" + str(i)},
                ),
                spec_hash="a" * 64,
            )[0]

        result = store.run_placement(transaction,worker_id="worker-"+str(i%nodes))
        return result, time.monotonic() - start

    try:
        start = time.monotonic()
        with ThreadPoolExecutor(32) as executor:
            results = list(executor.map(reserve, range(count)))
        elapsed = time.monotonic() - start
        assert all(result is not None for result, _ in results)
        timings = sorted(elapsed for _, elapsed in results)
        # Further simultaneous reservations must observe committed capacity.
        with ThreadPoolExecutor(32) as executor:
            overflow = list(executor.map(reserve, range(count, count + nodes * 2)))
        assert sum(r is not None for r, _ in overflow) == nodes * slots - count
        with store.pool.connection() as conn:
            sizes = conn.execute(
                "SELECT job_id,count(*) AS n FROM sandboxes GROUP BY job_id"
            ).fetchall()
            assert all(r["n"] <= slots for r in sizes)
        return {
            "backend": "canonical PostgresRoutingStore",
            "sandboxes": count,
            "nodes": nodes,
            "seconds": elapsed,
            "transactions_per_second": count / elapsed,
            "latency_seconds": {
                name: timings[min(len(timings) - 1, math.ceil(len(timings) * q) - 1)]
                for name, q in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)]
            },
            "overbooking": False,
            "serialization_retries": store.serialization_retries,
        }
    finally:
        store.close()
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sandboxes", type=int, default=512)
    parser.add_argument("--nodes", type=int, default=8)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = qualify(args.sandboxes, args.nodes)
    body = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(body)
    print(body, end="")
