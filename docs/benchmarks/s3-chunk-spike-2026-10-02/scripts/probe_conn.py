#!/usr/bin/env python3
"""Sequential 64 KiB ranged GETs on presigned URLs: keep-alive vs fresh TLS connection per request."""
import http.client, json, random, ssl, sys, time
meta = json.load(open("/root/s12/presigned.json"))
host, urls = meta["host"], [u for u in meta["urls"] if u["bytes"] > 8 << 20]
ctx = ssl.create_default_context()
rnd = random.Random(1)
N = int(sys.argv[1]) if len(sys.argv) > 1 else 40
out = {}
for mode in ("fresh", "keepalive", "fresh"):
    lat = []
    c = None
    for i in range(N):
        u = rnd.choice(urls)
        off = rnd.randrange(0, (u["bytes"] - 65536) // 4096) * 4096
        if c is None or mode == "fresh":
            if c:
                c.close()
            c = http.client.HTTPSConnection(host, 443, timeout=30, context=ctx)
        t0 = time.perf_counter()
        try:
            c.request("GET", u["url"], headers={"Range": f"bytes={off}-{off + 65535}"})
            r = c.getresponse(); b = r.read()
            lat.append(round((time.perf_counter() - t0) * 1000, 1))
        except Exception as e:
            lat.append(f"ERR {e!r}"[:60]); c = None
    ok = sorted(x for x in lat if isinstance(x, float))
    print(mode, "p50", ok[len(ok) // 2], "p90", ok[int(len(ok) * .9)], "max", ok[-1], "errs", N - len(ok), lat[:12], flush=True)
