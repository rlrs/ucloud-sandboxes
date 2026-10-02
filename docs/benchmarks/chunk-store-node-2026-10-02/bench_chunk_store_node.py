"""Local benchmarks for ucloud-chunk-store (C2.6): how to serve, and what to fill.

    uv run python docs/benchmarks/chunk-store-node-2026-10-02/bench_chunk_store_node.py serve --out serve.json
    uv run python docs/benchmarks/chunk-store-node-2026-10-02/bench_chunk_store_node.py fill --out fill.json

``serve``: 1 MiB range GETs against a warm cache (page cache) at concurrency
64-256, for the asyncio sendfile server (what ships), the first,
thread-per-connection server with sendfile or copying through Python
(threaded_baseline.py), and aiohttp's FileResponse. Clients are separate
processes; the server is its own process.

``fill``: a cold 500-sandbox burst against a fake S3 with S12's latency shape,
time-scaled by SCALE so a run takes minutes: TTFB lognormal (median 55 ms) for
90% of requests, 0.2-1.5 s for 8%, 3-15 s for 2%; 100 MB/s per stream, a
250 MiB/s aggregate cap, and a mid-body stall (2-10 s) with probability 1% per
8 MiB. Every latency reported is unscaled back to S12 time.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import multiprocessing
import os
from pathlib import Path
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

MIB = 1024 ** 2
READ, WRITE = "r" * 32, "w" * 32
SCALE = 0.2  # Simulated seconds per S12 second.


def percentiles(values):
    ordered = sorted(values)
    pick = lambda q: round(ordered[min(len(ordered) - 1, int(q * len(ordered)))] * 1000, 2)  # noqa: E731
    return {"n": len(ordered), "p50_ms": pick(.5), "p90_ms": pick(.9), "p99_ms": pick(.99),
            "max_ms": round(ordered[-1] * 1000, 2)} if ordered else {"n": 0}


# --- Fake S3 with S12's latency shape ---

class Bucket:
    def __init__(self, rate):
        self.rate, self.tokens, self.last, self.lock = rate, rate * .05, time.monotonic(), threading.Lock()

    def take(self, amount):
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.rate * .05, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                wait = (amount - self.tokens) / self.rate
            time.sleep(wait)


def fake_s3(port, sizes_file, scale, ready):
    sizes = json.loads(Path(sizes_file).read_text())
    data = random.Random(7).randbytes(64 * MIB)
    bucket, counters, guard = Bucket(250 * MIB / scale), {"requests": 0, "bytes": 0}, threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):  # noqa: N802
            if self.path == "/stats":
                payload = json.dumps(counters).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                return self.wfile.write(payload)
            key = self.path.split("?", 1)[0].split("/", 2)[2]  # /bucket/key
            size = sizes[key]
            first, _, last = self.headers["Range"].removeprefix("bytes=").partition("-")
            start, end = int(first), min(int(last), size - 1)
            rng = random.Random()
            roll = rng.random()
            ttfb = (rng.lognormvariate(-2.9, .3) if roll < .90 else rng.uniform(.2, 1.5) if roll < .98
                    else rng.uniform(3, 15))
            time.sleep(ttfb * scale)
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Content-Length", str(end - start + 1))
            self.end_headers()
            with guard:
                counters["requests"] += 1
            position, step = start, 256 * 1024
            try:
                while position <= end:
                    count = min(step, end + 1 - position)
                    bucket.take(count)
                    time.sleep(count / (100e6 / scale))
                    if rng.random() < .01 * count / (8 * MIB):
                        time.sleep(rng.uniform(2, 10) * scale)
                    offset = position % (len(data) - step)
                    self.wfile.write(data[offset:offset + count])
                    position += count
                    with guard:
                        counters["bytes"] += count
            except OSError:
                self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads, server.request_queue_size = True, 4096
    server.handle_error = lambda *_: None
    ready.set()
    server.serve_forever()


# --- A store node process ---

def store_process(args):
    from ucloud_sandboxes import chunk_store_node as node_module
    from ucloud_sandboxes.chunk_index import S3Presigner
    scale = args.scale
    node_module.HEDGE_MIN_SECONDS *= scale
    node_module.HEDGE_MAX_SECONDS *= scale
    node_module.ATTEMPT_READ_TIMEOUT_SECONDS = max(1.0, 20 * scale)
    node_module.MAX_HEDGES = args.hedges
    presigner = S3Presigner(f"http://127.0.0.1:{args.s3_port}", "bucket", "hel1", "AKID", "secret", path_style=True)
    source = node_module.S3Source(presigner, "p", concurrency=args.s3_concurrency, deadline=60.0)
    node = node_module.ChunkStoreNode(node_module.ExtentCache(args.cache, 200 * 1024 ** 3), source,
                                      extent_bytes=args.extent * MIB, warm_concurrency=16)
    if args.variant in ("threaded", "threaded-copy"):  # The first HTTP layer, for comparison.
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from threaded_baseline import Server
        server = Server(("127.0.0.1", args.port), node, READ, sendfile=args.variant == "threaded")
    else:  # What ships: one asyncio loop, sendfile.
        server = node_module.ChunkStoreServer(("127.0.0.1", args.port), node, read_token=READ, write_token=WRITE)
    print("ready", flush=True)
    server.serve_forever()


def aiohttp_process(args):
    from aiohttp import web

    async def handler(request):
        if request.headers.get("Authorization") != "Bearer " + READ:
            return web.Response(status=401)
        return web.FileResponse(Path(args.files) / request.match_info["name"])
    app = web.Application()
    app.router.add_get("/v1/objects/{name}", handler)
    print("ready", flush=True)
    web.run_app(app, host="127.0.0.1", port=args.port, print=None, access_log=None, backlog=4096)


def start(command, port):
    import socket
    process = subprocess.Popen([sys.executable, __file__, *command], stdout=subprocess.PIPE, text=True)
    assert process.stdout.readline().strip() == "ready", "server did not start"
    for _ in range(200):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return process
        except OSError:
            time.sleep(.05)
    raise RuntimeError("server did not listen")


def free_port():
    import socket
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# --- serve: warm-cache 1 MiB range GETs ---

def client_process(port, paths, sizes, threads, seconds, queue):
    latencies, firsts, errors = [], [], 0
    stop = time.monotonic() + seconds

    def run(seed):
        nonlocal errors
        rng, buffer = random.Random(seed), bytearray(MIB)
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        mine, first = [], True
        while time.monotonic() < stop:
            index = rng.randrange(len(paths))
            offset = rng.randrange(0, sizes[index] - MIB) // 4096 * 4096
            began = time.monotonic()
            try:
                connection.request("GET", paths[index], headers={"Range": f"bytes={offset}-{offset + MIB - 1}",
                                                                  "Authorization": "Bearer " + READ})
                response = connection.getresponse()
                view, got = memoryview(buffer), 0
                while got < MIB:
                    count = response.readinto(view[got:])
                    if not count:
                        break
                    got += count
                if response.status != 206 or got != MIB:
                    raise OSError(f"status {response.status}, {got} bytes")
                (firsts if first else mine).append(time.monotonic() - began)
                first = False
            except OSError:
                errors += 1
                connection.close()
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        latencies.extend(mine)
    with ThreadPoolExecutor(threads) as pool:
        list(pool.map(run, [random.random() for _ in range(threads)]))
    queue.put((latencies, firsts, errors))


def cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def serve_bench(args):
    root = Path(tempfile.mkdtemp(prefix="chunk-store-serve-"))
    results = {"machine": {"cpus": os.cpu_count(), "python": sys.version.split()[0]}, "runs": []}
    try:
        files, count, size = root / "files", args.objects, 64 * MIB
        files.mkdir()
        keys = [f"{index:064x}" for index in range(count)]
        sizes_file = root / "sizes.json"
        sizes_file.write_text(json.dumps({f"p/meta/{key}.boot.zst": size for key in keys}))
        s3_port, ready = free_port(), multiprocessing.Event()
        s3 = multiprocessing.Process(target=fake_s3, args=(s3_port, sizes_file, 1e-6, ready), daemon=True)
        s3.start()
        ready.wait()
        for variant in args.variants:
            port = free_port()
            if variant == "aiohttp":
                data = random.Random(7).randbytes(size)
                for key in keys:
                    (files / key).write_bytes(data)
                process = start(["aiohttp-server", "--port", str(port), "--files", str(files)], port)
                paths = [f"/v1/objects/{key}" for key in keys]
            else:
                cache = root / "cache"  # Filled once through the warm API, shared by the variants.
                if not cache.exists():
                    process = start(["store-server", "--port", str(port), "--s3-port", str(s3_port), "--cache",
                                     str(cache), "--extent", str(args.extent), "--scale", "1"], port)
                    from ucloud_sandboxes.chunk_store_node import ChunkStoreClient
                    client = ChunkStoreClient(f"http://127.0.0.1:{port}", WRITE)
                    job = client.warm([{"key": f"meta/{key}.boot.zst"} for key in keys], concurrency=16)
                    progress = client.wait(job["job"], timeout=600, poll=.2)
                    assert progress["state"] == "complete", progress
                    process.send_signal(signal.SIGTERM)
                    process.wait()
                    results["warm"] = progress
                port = free_port()
                process = start(["store-server", "--port", str(port), "--s3-port", str(s3_port), "--cache", str(cache),
                                 "--extent", str(args.extent), "--variant", variant, "--scale", "1"], port)
                paths = [f"/v1/objects/meta/{key}.boot.zst" for key in keys]
            # Page cache: read every byte once through the server.
            for path in paths:
                connection = http.client.HTTPConnection("127.0.0.1", port)
                connection.request("GET", path, headers={"Authorization": "Bearer " + READ,
                                                         "Range": f"bytes=0-{size - 1}"})
                assert len(connection.getresponse().read()) == size
                connection.close()
            for concurrency in args.concurrency:
                processes = min(args.client_processes, concurrency)
                queue, cpu = multiprocessing.Queue(), cpu_seconds(process.pid)
                began = time.monotonic()
                clients = [multiprocessing.Process(target=client_process, args=(
                    port, paths, [size] * len(paths), concurrency // processes, args.seconds, queue))
                    for _ in range(processes)]
                for client_process_ in clients:
                    client_process_.start()
                gathered = [queue.get() for _ in clients]
                for client_process_ in clients:
                    client_process_.join()
                wall = time.monotonic() - began
                latencies = [value for values, _, _ in gathered for value in values]
                firsts = [value for _, values, _ in gathered for value in values]
                run = {"variant": variant, "concurrency": concurrency, "seconds": round(wall, 2),
                       "requests": len(latencies), "errors": sum(errors for _, _, errors in gathered),
                       "first_request_on_new_connection": percentiles(firsts),
                       "throughput_mib_s": round(len(latencies) / wall, 1),
                       "server_cpu_cores": round((cpu_seconds(process.pid) - cpu) / wall, 2), **percentiles(latencies)}
                print(json.dumps(run), flush=True)
                results["runs"].append(run)
            process.send_signal(signal.SIGTERM)
            process.wait()
        s3.terminate()
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return results


# --- fill: a cold burst through the store at S12 latency ---

def burst_workload(seed=11):
    """64 tasks (images) x 8 rollouts on 3 workers. Images come in 8
    foundation groups of 8; a foundation has 10 packs of 32 MiB, an image 6 of
    its own of 8 MiB. A cold start reads 25 1 MiB windows (70% foundation),
    one after another. A worker's node cache dedupes an image's rollouts, so
    the store sees 3 readers per image (one per worker)."""
    rng = random.Random(seed)
    sizes, traces = {}, []
    foundations = [[f"meta/{rng.getrandbits(256):064x}.boot.zst" for _ in range(10)] for _ in range(8)]
    for group in foundations:
        sizes.update({key: 32 * MIB for key in group})
    for image in range(64):
        own = [f"meta/{rng.getrandbits(256):064x}.boot.zst" for _ in range(6)]
        sizes.update({key: 8 * MIB for key in own})
        trace = []
        for _ in range(25):
            key = rng.choice(foundations[image // 8]) if rng.random() < .7 else rng.choice(own)
            trace.append((key, rng.randrange(0, sizes[key] - MIB) // 4096 * 4096))
        traces.append(trace)
    return sizes, traces


def read_window(connection, path, offset, headers):
    connection.request("GET", path, headers={**headers, "Range": f"bytes={offset}-{offset + MIB - 1}"})
    response = connection.getresponse()
    body = response.read()
    if response.status != 206 or len(body) != MIB:
        raise OSError(f"status {response.status}")


def run_burst(base_port, traces, scale, prefix, headers):
    """Each image's 3 readers start within 10 s (scaled) and read in order."""
    rng, results, guard = random.Random(5), {"reader": [], "window": [], "errors": 0}, threading.Lock()
    starts = [(rng.uniform(0, 10 * scale), image) for image in range(len(traces)) for _ in range(3)]
    began = time.monotonic()

    def reader(item):
        delay, image = item
        time.sleep(delay)
        connection, windows, first = http.client.HTTPConnection("127.0.0.1", base_port, timeout=120), [], time.monotonic()
        for key, offset in traces[image]:
            started = time.monotonic()
            try:
                read_window(connection, prefix + key, offset, headers)
            except OSError:
                with guard:
                    results["errors"] += 1
                connection.close()
                connection = http.client.HTTPConnection("127.0.0.1", base_port, timeout=120)
            windows.append(time.monotonic() - started)
        with guard:
            results["reader"].append(time.monotonic() - first)
            results["window"].extend(windows)
    with ThreadPoolExecutor(len(starts)) as pool:
        list(pool.map(reader, starts))
    wall = time.monotonic() - began
    unscale = lambda summary: {name: round(value / scale, 1) if name.endswith("_ms") else value  # noqa: E731
                               for name, value in summary.items()}
    return {"burst_seconds": round(wall / scale, 1), "errors": results["errors"],
            "cold_start_storage_wait": unscale(percentiles(results["reader"])),
            "window": unscale(percentiles(results["window"]))}


def s3_stats(port):
    connection = http.client.HTTPConnection("127.0.0.1", port)
    connection.request("GET", "/stats")
    return json.loads(connection.getresponse().read())


def fill_bench(args):
    root = Path(tempfile.mkdtemp(prefix="chunk-store-fill-"))
    sizes, traces = burst_workload()
    unique = len({window for trace in traces for window in trace})
    needed = unique * MIB  # Bytes the readers need.
    results = {"scale": args.scale, "workload": {"images": 64, "readers": 192, "windows_per_reader": 25,
                                                 "unique_windows": unique,
                                                 "object_bytes": sum(sizes.values())}, "runs": []}
    try:
        sizes_file = root / "sizes.json"
        sizes_file.write_text(json.dumps({"p/" + key: size for key, size in sizes.items()}))
        for variant in args.variants:
            s3_port, ready = free_port(), multiprocessing.Event()
            s3 = multiprocessing.Process(target=fake_s3, args=(s3_port, sizes_file, args.scale, ready), daemon=True)
            s3.start()
            ready.wait()
            extent, hedges, warm, slots = variant
            began, warmed = time.monotonic(), None
            if extent == "direct":  # Phase A: workers range-read S3 themselves.
                run = run_burst(s3_port, traces, args.scale, "/bucket/p/", {})
                metrics = {}
            else:
                port = free_port()
                process = start(["store-server", "--port", str(port), "--s3-port", str(s3_port),
                                 "--cache", str(root / f"cache-{extent}-{hedges}"), "--extent", str(extent),
                                 "--hedges", str(hedges), "--scale", str(args.scale), "--s3-concurrency", str(slots)], port)
                from ucloud_sandboxes.chunk_store_node import ChunkStoreClient
                if warm:  # C9.3: the run's foundation packs, whole, before the burst.
                    client = ChunkStoreClient(f"http://127.0.0.1:{port}", WRITE)
                    job = client.warm([{"key": key} for key, size in sizes.items() if size == 32 * MIB],
                                      concurrency=16)
                    warmed = client.wait(job["job"], timeout=600, poll=.05)
                    warmed = {"seconds": round(warmed["seconds"] / args.scale, 1), "state": warmed["state"],
                              "gib": round(warmed["bytes"] / 1024 ** 3, 2)}
                run = run_burst(port, traces, args.scale, "/v1/objects/", {"Authorization": "Bearer " + READ})
                metrics = ChunkStoreClient(f"http://127.0.0.1:{port}", READ).metrics()
                process.send_signal(signal.SIGTERM)
                process.wait()
            stats = s3_stats(s3_port)
            s3.terminate()
            run = {"extent_mib": extent, "max_hedges": hedges, "s3_concurrency": slots, "warmed_foundations": warmed, **run,
                   "s3_gets": stats["requests"],
                   "s3_gib": round(stats["bytes"] / 1024 ** 3, 2), "amplification": round(stats["bytes"] / needed, 2),
                   "hedged": metrics.get("s3", {}).get("hedged"), "coalesced": metrics.get("coalesced"),
                   "wall_seconds": round(time.monotonic() - began, 1)}
            print(json.dumps(run), flush=True)
            results["runs"].append(run)
            shutil.rmtree(root / f"cache-{extent}-{hedges}", ignore_errors=True)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return results


def summarize(paths):
    """Markdown rows: the median of each fill metric over repeated runs."""
    import statistics
    runs = {}
    for path in paths:
        for run in json.loads(Path(path).read_text())["runs"]:
            label = f"{run['extent_mib']}" + ("" if run["extent_mib"] == "direct" else " MiB") + (
                ", no hedge" if run["max_hedges"] == 0 and run["extent_mib"] != "direct" else "") + (
                ", foundations warmed" if run.get("warmed_foundations") else "") + (
                f", {run['s3_concurrency']} S3 slots" if run.get("s3_concurrency", 64) != 64 else "")
            runs.setdefault(label, []).append(run)
    median = lambda values: statistics.median(values)  # noqa: E731
    for label, items in runs.items():
        cells = [median([run["burst_seconds"] for run in items]),
                 median([run["cold_start_storage_wait"]["p50_ms"] / 1000 for run in items]),
                 median([run["cold_start_storage_wait"]["p99_ms"] / 1000 for run in items]),
                 median([run["window"]["p50_ms"] for run in items]), median([run["window"]["p99_ms"] for run in items]),
                 median([run["window"]["max_ms"] for run in items]), median([run["s3_gets"] for run in items]),
                 median([run["amplification"] for run in items]), median([run["hedged"] or 0 for run in items])]
        print(f"| {label} | " + " | ".join(f"{value:,.1f}" if isinstance(value, float) else f"{value:,}"
                                           for value in cells) + f" | {len(items)} |")


def main():
    if sys.argv[1:2] == ["summarize"]:
        return summarize(sys.argv[2:])
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument("--variants", nargs="+", default=["asyncio", "threaded", "threaded-copy", "aiohttp"])
    serve.add_argument("--concurrency", nargs="+", type=int, default=[64, 128, 256])
    serve.add_argument("--objects", type=int, default=64)
    serve.add_argument("--extent", type=int, default=8)
    serve.add_argument("--seconds", type=float, default=15)
    serve.add_argument("--client-processes", type=int, default=8)
    serve.add_argument("--out", type=Path)
    fill = commands.add_parser("fill")
    fill.add_argument("--scale", type=float, default=SCALE)
    fill.add_argument("--variants", nargs="+", default=[
        "direct:0", "1:2", "4:2", "4:0", "8:2", "16:2", "64:2", "64:0", "4:2:warm", "64:2:warm"])
    fill.add_argument("--out", type=Path)
    store = commands.add_parser("store-server")
    for name in ("port", "s3-port", "extent"):
        store.add_argument("--" + name, type=int, required=True)
    store.add_argument("--cache", type=Path, required=True)
    store.add_argument("--variant", default="asyncio")
    store.add_argument("--hedges", type=int, default=2)
    store.add_argument("--scale", type=float, default=1.0)
    store.add_argument("--s3-concurrency", type=int, default=64)
    aio = commands.add_parser("aiohttp-server")
    aio.add_argument("--port", type=int, required=True)
    aio.add_argument("--files", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "store-server":
        return store_process(args)
    if args.command == "aiohttp-server":
        return aiohttp_process(args)
    if args.command == "fill":
        args.variants = [(value.split(":")[0] if value.startswith("direct") else int(value.split(":")[0]),
                          int(value.split(":")[1]), value.endswith(":warm"),
                          int(value.split(":c")[1]) if ":c" in value else 64) for value in args.variants]
    results = serve_bench(args) if args.command == "serve" else fill_bench(args)
    if args.out:
        args.out.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
