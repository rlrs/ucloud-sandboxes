"""Concurrent EnvironmentBackendClient calls against the real backend server (no spike retry)."""
import json, sys, threading
sys.path.insert(0, "/root/s11")
import s11lib as L
from ucloud_sandboxes.environment_backend import EnvironmentBackendClient
b = L.NbdBackend(); b.start()
client = EnvironmentBackendClient(L.ENVIO_SOCK)
res = {"ok": 0, "BlockingIOError": 0, "other": 0, "examples": []}
lock = threading.Lock(); go = threading.Event()
def worker():
    go.wait()
    try:
        client.metrics(); k = "ok"
    except BlockingIOError as exc:
        k = "BlockingIOError"; e = repr(exc)
    except Exception as exc:
        k = "other"; e = repr(exc)
    with lock:
        res[k] += 1
        if k != "ok" and len(res["examples"]) < 2: res["examples"].append(e)
ts = [threading.Thread(target=worker) for _ in range(40)]
[t.start() for t in ts]; go.set(); [t.join() for t in ts]
b.kill()
print(json.dumps(res))
json.dump(res, open("/root/s11/out/backend-client-eagain.json", "w"))
