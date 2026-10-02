import hashlib, os, sys, json, subprocess
sys.argv = ["mounttest.py", "47", "/data/work/dict-1m"]
src = open("/root/mounttest.py").read().split("def main():")[0]
exec(src)
def nyd(tag, run):
    x = Attach(tag)
    dev = x.nbd("nydus", f"{run}/images/047.boot", f"{run}/blobs")
    x.mount("-t", "erofs", "-o", "ro", dev, f"{x.base}/lower")
    return x
a = nyd("hd-dict", "/data/work/dict-1m"); b = nyd("hd-nodict", "/data/work/nodict-1m")
c, rc, _ = attach_today(47)
roots = {"dict": f"{a.base}/lower", "nodict": f"{b.base}/lower", "today": f"{c.base}/rootfs"}
diffs, n = [], 0
for dp, dn, fn in os.walk(roots["nodict"]):
    for f in fn:
        p = os.path.join(dp, f); rel = p[len(roots["nodict"]):]
        if os.path.islink(p) or not os.path.isfile(p):
            continue
        n += 1
        h = {}
        for k, r in roots.items():
            try:
                h[k] = hashlib.sha256(open(r + rel, "rb").read()).hexdigest()[:16]
            except OSError as e:
                h[k] = "ERR " + type(e).__name__
        if len(set(h.values())) > 1 or h["dict"].startswith("ERR"):
            diffs.append({"path": rel, **h, "size": os.path.getsize(p)})
print(json.dumps({"files": n, "diffs": diffs[:30], "ndiffs": len(diffs)}, indent=1))
for x in (a, b, c):
    x.close()
