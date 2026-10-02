#!/usr/bin/env python3
"""Cold vs repeated ranges: read N random (pack, offset) 64 KiB ranges, then the same ranges again,
then N ranges adjacent (+64 KiB) to the first set, sequentially on one keep-alive connection."""
import http.client, json, random, ssl, sys, time
meta = json.load(open("/root/s12/presigned.json"))
host, urls = meta["host"], [u for u in meta["urls"] if u["bytes"] > 8 << 20]
ctx = ssl.create_default_context()
rnd = random.Random(int(sys.argv[2]) if len(sys.argv) > 2 else 7)
N = int(sys.argv[1]) if len(sys.argv) > 1 else 30
pairs = [(u, rnd.randrange(0, (u["bytes"] - 262144) // 4096) * 4096) for u in (rnd.choice(urls) for _ in range(N))]
c = [http.client.HTTPSConnection(host, 443, timeout=60, context=ctx)]
def get(u, off, n=65536):
    t0 = time.perf_counter()
    for a in range(2):
        try:
            c[0].request("GET", u["url"], headers={"Range": f"bytes={off}-{off + n - 1}"})
            r = c[0].getresponse(); r.read()
            return round((time.perf_counter() - t0) * 1000, 1)
        except Exception:
            c[0] = http.client.HTTPSConnection(host, 443, timeout=60, context=ctx)
    return None
res = {}
for name, ps in (("cold", pairs), ("repeat", pairs), ("adjacent", [(u, o + 65536) for u, o in pairs]),
                 ("cold2", [(u, rnd.randrange(0, (u["bytes"] - 65536) // 4096) * 4096) for u, _ in pairs])):
    lat = [get(u, o) for u, o in ps]
    ok = sorted(x for x in lat if x is not None)
    res[name] = {"p50": ok[len(ok) // 2], "p90": ok[int(len(ok) * .9)], "max": ok[-1], "fail": N - len(ok)}
    print(name, res[name], lat[:10], flush=True)
