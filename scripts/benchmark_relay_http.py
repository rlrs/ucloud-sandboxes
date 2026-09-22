#!/usr/bin/env python3
"""Linux multiprocess relay qualification with real PostgreSQL and HTTP.

The worker wake is a timer. This measures relay coordination, NOT real sandbox
restore/tool latency. Use live_relay_load_benchmark.py for the product SLO.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import signal
import statistics
import sys
import tempfile
import time
from uuid import uuid4

from aiohttp import ClientConnectionError, ClientSession, TCPConnector, web
import psycopg
from psycopg import sql

from ucloud_sandboxes import model_relay as api
from ucloud_sandboxes.shared_control.credentials import read_private_dsn
from ucloud_sandboxes.shared_control.postgres import PostgresControlStore


def dsn(args):
    if args.dsn_file:
        return read_private_dsn(args.dsn_file)
    return os.environ["UCLOUD_TEST_POSTGRES_DSN"]


async def serve(args):
    async def park(request):
        return "qualification-epoch"

    async def wake(request):
        await asyncio.sleep(args.wake_ms / 1000)
        return "qualification-epoch"

    samples = []
    store = PostgresControlStore(
        dsn(args),
        "http-benchmark",
        schema=args.schema,
        max_connections=16,
        observe=samples.append,
    )
    app = api.create_model_relay_app(
        postgres_store=store,
        accepted_notifier=park,
        result_notifier=wake,
        sandbox_bearer_token="qualification-sandbox",
        worker_bearer_token="qualification-worker",
        request_timeout_seconds=120,
        maintenance_interval_seconds=0.25,
    )

    async def metrics(request):
        output = {}
        for operation in {x.operation for x in samples}:
            subset = [x for x in samples if x.operation == operation]
            output[operation] = {
                "count": len(subset),
                "pool_wait": quantiles([x.pool_wait_seconds for x in subset]),
                "transaction": quantiles([x.transaction_seconds for x in subset]),
                "commit": quantiles([x.commit_seconds for x in subset]),
            }
        return web.json_response(output)

    app.router.add_get("/qualification/metrics", metrics)
    # Short leases let the optional kill scenario finish quickly. Never overrides
    # a production configuration: this app/schema exists only for this run.
    app[api.STATE_KEY].claim_seconds = 1 if args.kill_relay else 30
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    print(
        json.dumps(
            {"port": site._server.sockets[0].getsockname()[1], "pid": os.getpid()}
        ),
        flush=True,
    )
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        await runner.cleanup()


def quantiles(values):
    values = sorted(values)
    return {
        "p50": statistics.median(values),
        "p95": values[min(len(values) - 1, int(len(values) * 0.95))],
        "p99": values[min(len(values) - 1, int(len(values) * 0.99))],
        "max": max(values),
    }


async def benchmark(args):
    if platform.system() != "Linux":
        raise ValueError("run this qualification on Linux")
    schema = "ucloud_shared_http_" + uuid4().hex
    store = PostgresControlStore(dsn(args), "http-benchmark", schema=schema)
    await store.open()
    await store.migrate()
    await store.close()
    processes = []
    logs = []
    result = {}
    started = time.time()
    source_hashes = {
        p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
        for p in (
            "ucloud_sandboxes/shared_control/relay.py",
            "ucloud_sandboxes/shared_control/postgres.py",
            "ucloud_sandboxes/shared_control/relay_schema.sql",
            "ucloud_sandboxes/model_relay.py",
            __file__,
        )
    }
    tasks = []
    try:
        with tempfile.TemporaryDirectory(prefix="relay-http-") as directory:
            urls = []
            for index in range(2):
                log = open(Path(directory) / f"server-{index}.log", "w+")
                logs.append(log)
                command = [
                    sys.executable,
                    __file__,
                    "--serve",
                    "--schema",
                    schema,
                    "--wake-ms",
                    str(args.wake_ms),
                ]
                if args.kill_relay:
                    command.append("--kill-relay")
                if args.dsn_file:
                    command.extend(["--dsn-file", str(args.dsn_file)])
                process = await asyncio.create_subprocess_exec(
                    *command, stdout=asyncio.subprocess.PIPE, stderr=log
                )
                processes.append(process)
                line = await asyncio.wait_for(process.stdout.readline(), 30)
                if not line:
                    log.seek(0)
                    raise RuntimeError("relay child failed: " + log.read()[-2000:])
                urls.append("http://127.0.0.1:" + str(json.loads(line)["port"]))
            auth = {"Authorization": "Bearer qualification-worker"}
            sandbox_auth = {"Authorization": "Bearer qualification-sandbox"}
            # Distinct bodies detect cross-request delivery, not just byte counts.
            expected = []
            for i in range(args.agents):
                marker = hashlib.sha256(str(i).encode()).digest()
                expected.append(
                    (marker * ((args.response_bytes + 31) // 32))[: args.response_bytes]
                )
            async with ClientSession(connector=TCPConnector(limit=0)) as session:

                async def json_request(method, url, **kwargs):
                    async with session.request(
                        method, url, headers=auth, **kwargs
                    ) as response:
                        body = await response.read()
                        if response.status >= 400:
                            raise RuntimeError(
                                f"HTTP {response.status}: {body[:200]!r}"
                            )
                        return json.loads(body)

                async def register(i):
                    reply = await json_request(
                        "POST",
                        urls[i % 2] + "/v1/relay/rollouts",
                        json={
                            "rollout_id": f"agent-{i}",
                            "metadata": {
                                "sandbox_id": f"sandbox-{i}",
                                "sandbox_generation": 1,
                                api.AGENT_LIFECYCLE_METADATA_KEY: api.MANAGED_AGENT_LIFECYCLE,
                            },
                        },
                    )
                    return reply["rollout"]["registration_token"]

                tokens = await asyncio.gather(
                    *(register(i) for i in range(args.agents))
                )
                ready_at = {}
                latencies = {}
                retries = {"caller": 0, "response": 0}

                async def caller(i):
                    target = i % 2
                    while True:
                        try:
                            async with session.post(
                                urls[target] + f"/tunnels/agent-{i}/model",
                                data=b"prompt",
                                headers={
                                    **sandbox_auth,
                                    api.RELAY_REQUEST_ID_HEADER: f"request-{i}",
                                },
                            ) as response:
                                body = await response.read()
                                if response.status != 200 or body != expected[i]:
                                    raise RuntimeError(
                                        f"caller {i} mismatch/status {response.status}"
                                    )
                                latencies[i] = time.monotonic() - ready_at[i]
                                return
                        except (OSError, asyncio.TimeoutError, ClientConnectionError):
                            if not args.kill_relay:
                                raise
                            retries["caller"] += 1
                            target = 1
                            await asyncio.sleep(0.02)

                tasks = [asyncio.create_task(caller(i)) for i in range(args.agents)]

                async def poll(i):
                    reply = await json_request(
                        "GET",
                        urls[(i + 1) % 2] + "/worker/poll",
                        params={
                            "rollout_id": f"agent-{i}",
                            "registration_token": tokens[i],
                            "timeout_seconds": 30,
                        },
                    )
                    (request,) = reply["requests"]
                    return request

                requests = await asyncio.gather(*(poll(i) for i in range(args.agents)))
                # All inference results offered together; no hidden parked-state wait.
                offered = time.monotonic()
                ready_at.update({i: offered for i in range(args.agents)})

                async def respond(i):
                    target = (i + 1) % 2
                    while True:
                        try:
                            return await json_request(
                                "POST",
                                urls[target] + "/worker/respond",
                                json={
                                    "request_id": requests[i]["request_id"],
                                    "registration_token": tokens[i],
                                    "lease_id": requests[i]["lease_id"],
                                    "body": api._encoded_body(expected[i]),
                                },
                            )
                        except (OSError, asyncio.TimeoutError, ClientConnectionError):
                            if not args.kill_relay:
                                raise
                            retries["response"] += 1
                            target = 1
                            await asyncio.sleep(0.05)

                commits = [asyncio.create_task(respond(i)) for i in range(args.agents)]
                tasks.extend(commits)
                if args.kill_relay:
                    await asyncio.sleep(0.1)
                    processes[0].kill()
                    await processes[0].wait()
                await asyncio.wait_for(asyncio.gather(*tasks), 90)
                stats = await json_request("GET", urls[1] + "/v1/relay/stats")
                if stats["delivery_pending"] or stats["inflight"]:
                    raise RuntimeError("durable work did not drain")
                metrics = [
                    await json_request("GET", url + "/qualification/metrics")
                    for index, url in enumerate(urls)
                    if not args.kill_relay or index == 1
                ]
                result = {
                    "metrics": metrics,
                    "agents": args.agents,
                    "servers": 2,
                    "responses_verified": len(latencies),
                    "response_ready_to_http_response_seconds": quantiles(
                        list(latencies.values())
                    ),
                    "elapsed_response_burst_seconds": time.monotonic() - offered,
                    "simulated_wake_ms": args.wake_ms,
                    "killed_relay": args.kill_relay,
                    "connection_retries": retries,
                    "response_bytes": args.response_bytes,
                    "stats": stats,
                    "limitations": [
                        "Worker wake is simulated; no runsc, storage restore or tool execution.",
                        "Two separate relay processes and PostgreSQL run on one Linux host.",
                    ],
                    "linux": platform.platform(),
                    "python": platform.python_version(),
                    "started_at_unix": started,
                    "source_sha256": source_hashes,
                }
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for process in processes:
            if process.returncode is None:
                process.terminate()
        for process in processes:
            try:
                await asyncio.wait_for(process.wait(), 10)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        for log in logs:
            if not result:
                log.seek(0)
                print(log.read()[-16000:], file=sys.stderr)
            log.close()
        async with await psycopg.AsyncConnection.connect(
            dsn(args), autocommit=True
        ) as conn:
            await conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-file", type=Path)
    parser.add_argument("--agents", type=int, default=512)
    parser.add_argument("--wake-ms", type=float, default=600)
    parser.add_argument("--response-bytes", type=int, default=32768)
    parser.add_argument("--kill-relay", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--schema")
    args = parser.parse_args()
    if (
        args.agents < 1
        or args.wake_ms < 0
        or not 0 <= args.response_bytes <= api.MAX_WORKER_RESPONSE_BYTES
    ):
        parser.error("invalid benchmark size")
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(max(soft, 8192), hard), hard))
    asyncio.run(serve(args) if args.serve else benchmark(args))


if __name__ == "__main__":
    main()
