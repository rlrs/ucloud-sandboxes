#!/usr/bin/env python3
"""S12 (b): direct ranged GETs on presigned URLs, as a worker would issue them.

  s3bench.py --urls /root/s12/presigned.json --out /data/results/s3bench-<client>.json
             [--start-at <unix time>] [--sizes 65536,1048576,4194304] [--conc 1,8,32,64,128,256]
             [--seconds 20]

Each cell (size x concurrency) runs `--seconds` with `conc` in-flight requests spread over
processes (<= 16 threads each), on per-thread keep-alive TLS connections. A request picks a random
pack and a random 4 KiB-aligned offset. Records per-request TTFB (request sent -> status line) and
total latency, status codes (503 SlowDown counted separately), errors, bytes and client CPU.
--start-at aligns cells across the three clients (each cell starts at start_at + k * (seconds + gap)).
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import resource
import sys
import threading
import time

sys.path.insert(0, "/root/s12")
import s3lib  # noqa: E402

GAP = 5.0


def worker(urls, host, size, threads, deadline, warm_until, seed, q):
    r = s3lib.PresignedReader(host)
    out = {"lat": [], "ttfb": [], "status": {}, "errors": 0, "err_samples": [], "bytes": 0, "short": 0}
    lock = threading.Lock()
    rnd = random.Random(seed)
    big = [u for u in urls if u["bytes"] >= size + 4096]

    def loop(tid):
        rr = random.Random(seed * 1000 + tid)
        while time.time() < deadline:
            u = rr.choice(big)
            off = rr.randrange(0, (u["bytes"] - size) // 4096) * 4096
            try:
                st, body, ttfb, total = r.get(u["url"], off, size)
            except Exception as e:  # noqa: BLE001
                with lock:
                    out["errors"] += 1
                    if len(out["err_samples"]) < 5:
                        out["err_samples"].append(repr(e)[:200])
                continue
            if time.time() < warm_until:
                continue  # discard the connection warm-up second
            with lock:
                out["status"][st] = out["status"].get(st, 0) + 1
                if st == 206:
                    out["lat"].append(total); out["ttfb"].append(ttfb); out["bytes"] += len(body)
                    out["short"] += len(body) != size
                elif len(out["err_samples"]) < 5:
                    out["err_samples"].append(f"{st} {body[:200]!r}")
    ts = [threading.Thread(target=loop, args=(k,)) for k in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    ru = resource.getrusage(resource.RUSAGE_SELF)
    out["cpu_s"] = ru.ru_utime + ru.ru_stime
    out["connects"] = r.connects
    q.put(out)


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def cell(urls, host, size, conc, seconds, start):
    procs_n = max(1, min(16, (conc + 15) // 16)) if conc >= 16 else 1
    per = [conc // procs_n + (1 if k < conc % procs_n else 0) for k in range(procs_n)]
    while time.time() < start:
        time.sleep(min(0.05, start - time.time()))
    t0 = time.time()
    warm_until = t0 + 1.0
    deadline = t0 + seconds
    q = mp.Queue()
    ps = [mp.Process(target=worker, args=(urls, host, size, n, deadline, warm_until, random.randrange(1 << 30), q))
          for n in per]
    for p in ps:
        p.start()
    res = [q.get() for _ in ps]
    for p in ps:
        p.join()
    wall = time.time() - warm_until
    lat = [x for r in res for x in r["lat"]]
    ttfb = [x for r in res for x in r["ttfb"]]
    status = {}
    for r in res:
        for k, v in r["status"].items():
            status[str(k)] = status.get(str(k), 0) + v
    nbytes = sum(r["bytes"] for r in res)
    return {"size": size, "conc": conc, "procs": procs_n, "measured_s": wall, "requests_ok": len(lat),
            "req_per_s": len(lat) / wall, "MiB_per_s": nbytes / wall / 2 ** 20,
            "ttfb_ms": {p: (pct(ttfb, q_) or 0) * 1000 for p, q_ in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))},
            "total_ms": {p: (pct(lat, q_) or 0) * 1000 for p, q_ in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))},
            "status": status, "slowdown_503": status.get("503", 0), "errors": sum(r["errors"] for r in res),
            "short_bodies": sum(r["short"] for r in res),
            "err_samples": [s for r in res for s in r["err_samples"]][:8],
            "client_cpu_s": sum(r["cpu_s"] for r in res), "connects": sum(r["connects"] for r in res),
            "start": t0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--start-at", type=float, default=0)
    ap.add_argument("--sizes", default="65536,1048576,4194304")
    ap.add_argument("--conc", default="1,8,32,64,128,256")
    ap.add_argument("--seconds", type=float, default=20)
    ap.add_argument("--client", default=os.uname().nodename)
    args = ap.parse_args()
    meta = json.load(open(args.urls))
    urls, host = meta["urls"], meta["host"]
    start = args.start_at or time.time() + 1
    cells = []
    k = 0
    for size in map(int, args.sizes.split(",")):
        for conc in map(int, args.conc.split(",")):
            c = cell(urls, host, size, conc, args.seconds, start + k * (args.seconds + GAP))
            k += 1
            c["client"] = args.client
            cells.append(c)
            print(json.dumps({x: c[x] for x in ("size", "conc", "requests_ok", "req_per_s", "MiB_per_s", "ttfb_ms",
                                                "total_ms", "slowdown_503", "errors", "client_cpu_s")}), flush=True)
            json.dump({"client": args.client, "host": host, "cells": cells}, open(args.out, "w"), indent=1)


if __name__ == "__main__":
    main()
