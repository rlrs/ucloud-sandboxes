"""Raw exec starts through the gateway; aggregate node timings. usage: url token n_sandboxes concurrency"""
import json, urllib.error, sys, time, urllib.request, collections
from concurrent.futures import ThreadPoolExecutor
url, token, n, conc = sys.argv[1], open(sys.argv[2]).read().strip(), int(sys.argv[3]), int(sys.argv[4])
def one(i):
    sid = f"exec-probe-{i % n}"
    req = urllib.request.Request(f"{url}/v1/sandboxes/{sid}/exec?initial_wait_seconds=0.05", method="POST",
        data=json.dumps({"command": ["sh", "-c", "printf ready"], "env": {}, "working_dir": None, "stdin": False, "tty": False}).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = {"error": e.code, "detail": e.read()[:300].decode(errors="replace")}
    return (time.monotonic() - t0) * 1000, body
with ThreadPoolExecutor(conc) as pool:
    rows = list(pool.map(one, range(conc)))
errs=collections.Counter(str(b.get("error"))+" "+b.get("detail","")[:200] for _,b in rows if "error" in b); print("errors", dict(errs))
rows=[r for r in rows if "error" not in r[1]] or rows
print("keys", list(rows[0][1].keys()), json.dumps(rows[0][1].get("timings"))[:1500])
agg = collections.defaultdict(list)
for wall, body in rows:
    agg["client_wall_ms"].append(wall)
    t = body.get("timings") or {}
    for group in ("manager", "session_start"):
        for k, v in (t.get(group) or {}).items():
            if isinstance(v, (int, float)): agg[f"{group}.{k}"].append(v)
    if "start_ms" in t: agg["start_ms"].append(t["start_ms"])
for k, v in sorted(agg.items()):
    v.sort(); print(f"{k:50s} p50={v[len(v)//2]:8.1f} p95={v[int(len(v)*.95)]:8.1f} max={v[-1]:8.1f}")
