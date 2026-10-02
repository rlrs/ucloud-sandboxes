"""Our NBD path: backend downloaded_bytes vs loopback counter for one attach + reads."""
import json, os, sys, time
sys.path.insert(0, "/root/s11")
import s11lib as L
img = [i for i in json.load(open("/root/s11/images.json"))["images"] if i["name"] == "scaleswe-responses-0"][0]
L.drop_caches()
b = L.NbdBackend(); b.start(); f = L.NbdFrontend()
c0 = L.nft_counters()["proxy_tx"]
mnt, t = f.attach(img["prepared_reference"])
c1 = L.nft_counters()["proxy_tx"]; m1 = f.metrics()
n = 0
for dirpath, _, fns in os.walk(mnt / "usr/local/lib"):
    for fn in fns:
        p = os.path.join(dirpath, fn)
        if os.path.isfile(p) and not os.path.islink(p):
            with open(p, "rb") as fh: n += len(fh.read())
c2 = L.nft_counters()["proxy_tx"]; m2 = f.metrics()
out = {"attach_counter": c1 - c0, "attach_downloaded": m1["downloaded_bytes"], "file_bytes_read": n,
       "read_counter": c2 - c1, "read_downloaded": m2["downloaded_bytes"] - m1["downloaded_bytes"],
       "misses": m2["misses"], "hits": m2["hits"]}
print(json.dumps(out))
json.dump(out, open("/root/s11/out/nbd-bytes-check.json", "w"))
L.unmount_all(L.ENVSTORE_ROOT); L.unmount_all(L.ENVIO_ROOT); b.kill()
