"""Replay bench.py's seq flow for two images on both paths, printing counter vs daemon accounting."""
import json, sys
sys.argv = ["bench.py", "nbd", "seq"]
sys.path.insert(0, "/root/s11")
import bench as B
import s11lib as L
out = []
for P in (B.Nbd, B.Fscache):
    p = P()
    try:
        for img in [i for i in B.IMAGES if i["name"] in ("scaleswe-responses-0", "tmax-1")]:
            p.reset()
            a = B.snapshot(p)
            r = B.one_sandbox(p, img, "s11-check-" + p.name + img["name"][-1])
            b = B.snapshot(p)
            m = p.daemon_metrics()
            r.pop("_sandbox").stop()
            rec = {"path": p.name, "image": img["name"], "counter": b["bytes"] - a["bytes"],
                   "daemon": m.get("downloaded_bytes") if p.name == "nbd" else m["/api/v1/metrics/backend"]["read_amount_total"]}
            print(json.dumps(rec), flush=True); out.append(rec)
    finally:
        p.close()
json.dump(out, open("/root/s11/out/bytes-check2.json", "w"), indent=1)
