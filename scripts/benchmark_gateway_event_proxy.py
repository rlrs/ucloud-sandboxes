"""Compare real gateway event HTTP paths on an isolated Linux machine.

Uses production parsing/authentication/route stores/worker transport and separate
processes for worker and gateway. It is a component qualification, not a sandbox
wake benchmark. No production credentials or deployment are used.
"""

from __future__ import annotations

from collections import Counter
import argparse
import asyncio
import json
import multiprocessing
import os
from pathlib import Path
import statistics
from tempfile import TemporaryDirectory
from threading import Thread
import time


def worker_process(pipe, hold):
    from aiohttp import web

    async def events(request):
        if request.headers.get("Authorization") != "Bearer node-private":
            raise web.HTTPUnauthorized()
        await asyncio.sleep(hold)
        return web.json_response({"events": [], "final": True, "padding": "x" * 512})

    async def run():
        app = web.Application()
        app.router.add_get("/v1/exec/{session}/events", events)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        pipe.send(site._server.sockets[0].getsockname()[1])
        await asyncio.to_thread(pipe.recv)
        await runner.cleanup()

    asyncio.run(run())


def gateway_process(pipe, root, node_port, asynchronous, concurrency, cpus):
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, set(sorted(os.sched_getaffinity(0))[:cpus]))
    from ucloud_sandboxes.control_plane import build_server
    from ucloud_sandboxes.models import NodeHeartbeat, utc_now
    from ucloud_sandboxes.routing import ExecRoute

    root = Path(root)
    server = build_server(
        "127.0.0.1",
        0,
        root / "control.sqlite",
        gateway_bearer_token="operator",
        sandbox_api_token="public",
        heartbeat_bearer_token="heartbeat",
        node_control_bearer_token="node-private",
        deployment_id="qualification",
        routing_file=root / "routing.sqlite",
        image_file=root / "images.json",
        metrics_file=root / "metrics.sqlite",
        async_proxy_responses=asynchronous,
        max_http_request_threads=max(1024, concurrency + 16),
    )
    node_url = f"http://127.0.0.1:{node_port}"
    server.RequestHandlerClass.store.receive_heartbeat(
        NodeHeartbeat(
            node_id="worker",
            job_id="job",
            updated_at=utc_now(),
            received_at=utc_now(),
            node_url=node_url,
            active_sandboxes=concurrency,
            deployment_id="qualification",
        )
    )
    for index in range(concurrency):
        server.RequestHandlerClass.routing_store.upsert_exec(
            ExecRoute(
                session_id=f"s{index}",
                sandbox_id=f"sandbox{index}",
                node_id="worker",
                job_id="job",
                node_url=node_url,
            )
        )
    thread = Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.001}, daemon=True
    )
    thread.start()
    pipe.send(server.server_port)
    pipe.recv()
    server.shutdown()
    server.server_close()
    thread.join()


def process_usage(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"), int(
        fields[17]
    )


async def exercise(port, pid, concurrency, rounds):
    import aiohttp

    latencies, health, errors, threads, descriptors = [], [], [], [], []
    def fd_types():
        counts = Counter()
        for descriptor in Path(f"/proc/{pid}/fd").iterdir():
            try:
                target = descriptor.readlink().as_posix()
            except FileNotFoundError:
                continue
            counts["socket" if target.startswith("socket:") else ("database" if ".sqlite" in target else target)] += 1
        return dict(counts)

    peak_fd_types = {}
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=concurrency + 16),
        timeout=aiohttp.ClientTimeout(total=30),
    ) as session:

        async def sample():
            nonlocal peak_fd_types
            while True:
                threads.append(process_usage(pid)[1])
                count = len(list(Path(f"/proc/{pid}/fd").iterdir()))
                if not descriptors or count > max(descriptors):
                    peak_fd_types = fd_types()
                descriptors.append(count)
                started = time.monotonic()
                async with session.get(f"http://127.0.0.1:{port}/healthz") as reply:
                    await reply.read()
                    if reply.status != 200:
                        errors.append(f"health HTTP {reply.status}")
                health.append(time.monotonic() - started)
                await asyncio.sleep(0.02)

        async def poll(index):
            for _ in range(rounds):
                started = time.monotonic()
                async with session.get(
                    f"http://127.0.0.1:{port}/v1/exec/s{index}/events?wait=1",
                    headers={"Authorization": "Bearer public"},
                ) as reply:
                    body = await reply.json()
                    if reply.status != 200 or body.get("final") is not True:
                        errors.append({"status": reply.status, "body": body})
                latencies.append(time.monotonic() - started)

        # Warm transport, stores and request worker reuse before measuring.
        await asyncio.gather(*(poll(index) for index in range(min(8, concurrency))))
        latencies.clear()
        cpu0, _ = process_usage(pid)
        wall0 = time.monotonic()
        monitor = asyncio.create_task(sample())
        try:
            await asyncio.gather(*(poll(index) for index in range(concurrency)))
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
        wall = time.monotonic() - wall0
        cpu = process_usage(pid)[0] - cpu0

    def summary(values):
        ordered = sorted(values)
        return {
            "p50": statistics.median(ordered),
            "p95": ordered[int(0.95 * (len(ordered) - 1))],
            "max": max(ordered),
        }

    return {
        "count": len(latencies),
        "errors": errors,
        "wall_seconds": wall,
        "gateway_cpu_seconds": cpu,
        "gateway_cpu_seconds_per_request": cpu / len(latencies),
        "gateway_peak_threads": max(threads),
        "gateway_peak_descriptors": max(descriptors),
        "gateway_final_descriptors": len(list(Path(f"/proc/{pid}/fd").iterdir())),
        "gateway_peak_fd_types": peak_fd_types,
        "gateway_final_fd_types": fd_types(),
        "gateway_limits": Path(f"/proc/{pid}/limits").read_text(),
        "latency_seconds": summary(latencies),
        "health_seconds": summary(health),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, default=512)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--hold-seconds", type=float, default=0.2)
    parser.add_argument("--gateway-cpus", type=int, default=4)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    worker = context.Process(target=worker_process, args=(child, args.hold_seconds))
    worker.start()
    node_port = parent.recv()
    results = {"configuration": vars(args) | {"output": str(args.output)}, "runs": []}
    try:
        # Reverse order in a second pair to reduce ordering/cache bias.
        for asynchronous in (False, True, True, False):
            with TemporaryDirectory() as root:
                control, peer = context.Pipe()
                process = context.Process(
                    target=gateway_process,
                    args=(
                        peer,
                        root,
                        node_port,
                        asynchronous,
                        args.concurrency,
                        args.gateway_cpus,
                    ),
                )
                process.start()
                try:
                    port = control.recv()
                    result = asyncio.run(
                        exercise(port, process.pid, args.concurrency, args.rounds)
                    )
                    result["asynchronous"] = asynchronous
                    results["runs"].append(result)
                    print(json.dumps(result), flush=True)
                finally:
                    if process.is_alive():
                        control.send("stop")
                    process.join(10)
                    if process.is_alive():
                        process.terminate()
                        process.join()
            args.output.write_text(json.dumps(results, indent=2) + "\n")
    finally:
        parent.send("stop")
        worker.join(10)
        if worker.is_alive():
            worker.terminate()
            worker.join()


if __name__ == "__main__":
    main()
