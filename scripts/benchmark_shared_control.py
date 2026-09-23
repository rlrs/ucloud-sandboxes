#!/usr/bin/env python3
"""Measure PostgreSQL coordination with simulated workers, never production load.

PostgreSQL uses a fresh private schema, dropped at completion.
This is NOT an end-to-end sandbox restore or a same-algorithm database shootout.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import sys
import time
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))



def summary(samples):
    values = sorted(samples)
    return {"count": len(values), "p50": statistics.median(values) if values else None,
            "p95": values[math.ceil(len(values) * .95) - 1] if values else None,
            "p99": values[math.ceil(len(values) * .99) - 1] if values else None,
            "max": max(values, default=None)}


async def postgres_trial(args, body):
    import psycopg
    from psycopg import sql
    from ucloud_sandboxes.shared_control import fixtures
    from ucloud_sandboxes.shared_control.dispatcher import WakeDispatcher
    from ucloud_sandboxes.shared_control.model import WakeProof
    from ucloud_sandboxes.shared_control.qualification import QualificationControlStore

    dsn = args.dsn_file.read_text().strip()
    schema = "ucloud_shared_bench_" + uuid4().hex
    samples = []
    stores = [QualificationControlStore(dsn, "benchmark", schema=schema, max_connections=args.connections,
                                  observe=samples.append) for _ in range(args.dispatchers)]
    tasks = []
    stop = asyncio.Event()
    try:
        for store in stores:
            await store.open()
        store = stores[0]
        await store.migrate()
        for index in range(args.nodes):
            # Remove artificial capacity pressure from this coordination-only run.
            await fixtures.node(store, f"n{index}", budget_mb=args.agents * 128)
        async def seed(index):
            await fixtures.sandbox(store, f"s{index}", f"n{index % args.nodes}")
            await fixtures.request(store, f"r{index}", f"s{index}")
        await asyncio.gather(*(seed(i) for i in range(args.agents)))
        samples.clear()
        starts, elapsed, commits = {}, {}, []
        finished = asyncio.Event()
        def completed(op):
            elapsed[op.sandbox_id] = time.monotonic() - starts[op.sandbox_id]
            if len(elapsed) == args.agents:
                finished.set()
        async def worker(op):
            await asyncio.sleep(args.restore_ms / 1000)
            return WakeProof(op.operation_id, op.generation, op.node_epoch, op.lifecycle_sequence, 1)
        dispatchers = [WakeDispatcher(s, worker, concurrency=args.connections, on_completed=completed) for s in stores]
        tasks = [asyncio.create_task(d.run(stop)) for d in dispatchers]
        started = time.monotonic()
        async def submit(i):
            offered = started + i / args.arrival_rate if args.arrival_rate else started
            await asyncio.sleep(max(0, offered - time.monotonic()))
            starts[f"s{i}"] = offered
            await stores[i % len(stores)].accept_result(f"r{i}", registration_id="registration-1", lease_id="lease-1", body=body)
            commits.append(time.monotonic() - starts[f"s{i}"])
        await asyncio.gather(*(submit(i) for i in range(args.agents)))
        waiter = asyncio.create_task(finished.wait())
        done, _pending = await asyncio.wait([waiter, *tasks], timeout=120, return_when=asyncio.FIRST_COMPLETED)
        if waiter not in done:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            for task in done:
                task.result()
            raise TimeoutError("coordination benchmark did not finish")
        total_seconds = time.monotonic() - started
        stop.set()
        await asyncio.gather(*tasks)
        measured = list(samples)
        snapshot = await store.snapshot()
        assert snapshot["operations"] == {"succeeded": args.agents}, snapshot
        assert all(n["reserved_restore_mb"] == 0 for n in snapshot["nodes"])
        for i in range(args.agents):
            assert await store.read_result(f"r{i}", registration_id="registration-1") == body
        by_operation = defaultdict(list)
        for sample in measured:
            by_operation[sample.operation].append(sample)
        metrics = {name: {field: summary([getattr(s, field) for s in group])
                         for field in ("pool_wait_seconds", "transaction_seconds", "commit_seconds", "lock_query_seconds")}
                   for name, group in by_operation.items()}
        return {"backend": "postgres", "correct": True, "seconds": total_seconds,
                "result_commit_seconds": summary(commits), "ready_seconds": summary(elapsed.values()),
                "transactions": metrics, "sample_count": len(measured)}
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for store in stores:
            await store.close()
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))




async def run(args):
    trials = []
    source_sha256 = {str(p.relative_to(Path(__file__).resolve().parents[1])): hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in [Path(__file__).resolve(), *sorted((Path(__file__).resolve().parents[1] / "ucloud_sandboxes/shared_control").glob("*.py")),
                               Path(__file__).resolve().parents[1] / "ucloud_sandboxes/shared_control/schema.sql"]}
    body = b"x" * args.response_bytes
    for repeat in range(args.repeats):
        trial = await postgres_trial(args, body)
        trial["repeat"] = repeat
        trials.append(trial)
        print(json.dumps({"backend": trial["backend"], "repeat": repeat, "ready_seconds": trial["ready_seconds"]}), flush=True)
    return {"created_at": datetime.now(timezone.utc).isoformat(), "kind": "coordination-only-simulated-worker",
            "configuration": {k: v for k, v in vars(args).items() if k not in ("dsn_file", "output")},
            "limitations": ["No real sandbox, worker storage or network latency.",
                            "This is not a full production qualification or a same-algorithm database comparison."],
            "python": sys.version.split()[0], "platform": platform.platform(),
            "source_sha256": source_sha256,
            "trials": trials}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--agents", type=int, default=256)
    parser.add_argument("--nodes", type=int, default=4)
    parser.add_argument("--connections", type=int, default=16)
    parser.add_argument("--dispatchers", type=int, default=2)
    parser.add_argument("--response-bytes", type=int, default=32768)
    parser.add_argument("--restore-ms", type=float, default=0)
    parser.add_argument("--arrival-rate", type=float, default=0, help="offered results/second; zero is a synchronized burst")
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    for key in ("agents", "nodes", "connections", "dispatchers", "response_bytes", "repeats"):
        if getattr(args, key) <= 0:
            parser.error(key + " must be positive")
    if not math.isfinite(args.restore_ms) or not 0 <= args.restore_ms < 10000:
        parser.error("restore-ms must be finite and between 0 and 10000")
    if args.response_bytes > 32 * 1024 * 1024:
        parser.error("response exceeds relay body limit")
    if not math.isfinite(args.arrival_rate) or args.arrival_rate < 0:
        parser.error("arrival-rate must be finite and nonnegative")
    result = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
