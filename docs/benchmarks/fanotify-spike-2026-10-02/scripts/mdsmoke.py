#!/usr/bin/env python3
"""S13 smoke test: fand.py multidev, odirect and unmark-when-full paths on one image (idx 130) against gate 1's complete
unified RAFS file. Separate socket and directories, so it can run beside other work."""
import json
import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, "/root/s13")
from common import compare_hashes, hash_tree, mount_erofs, sh, umount  # noqa: E402

SOCK = "/run/s13smoke.sock"
D = "/data/s13/smoke"
out = {}
mnt = "/mnt/s13smoke"
os.makedirs(D, exist_ok=True)
assert mount_erofs("/data/s13/g1/rafs-full.img", mnt)["rc"] == 0
ref, _, _ = hash_tree(mnt)
umount(mnt)
for label, extra in (("multidev", []), ("image-odirect", ["--odirect"]), ("image-unmark", ["--unmark-when-full"])):
    sh("rm", "-rf", D)
    os.makedirs(D)
    p = subprocess.Popen(["python3", "/root/s13/fand.py", "--sock", SOCK, *extra], stdout=subprocess.PIPE, text=True)
    assert p.stdout.readline().startswith("LISTENING")
    s = socket.socket(socket.AF_UNIX)
    s.connect(SOCK)
    f = s.makefile("rb")

    def call(**req):
        s.sendall((json.dumps(req) + "\n").encode())
        return json.loads(f.readline())
    if label == "multidev":
        r = call(op="attach_multidev", name="img-130", dir=D)
        mo = mount_erofs(r["boot"], mnt, "ro," + ",".join(f"device={d}" for d in r["devices"]))
    else:
        r = call(op="attach_image", name="img-130", path=f"{D}/img.img")
        mo = mount_erofs(r["path"], mnt, "ro,directio")
    t = time.time()
    got, errs, _ = hash_tree(mnt)
    rec = {"attach": {k: v for k, v in r.items() if k != "devices"}, "mount": mo, "read_s": time.time() - t,
           "content": compare_hashes(ref, got), "errors": len(errs)}
    st = call(op="stats", reset=True)
    rec["stats"] = {k: st[k] for k in ("counters", "event_ms", "event_range")}
    # Re-read everything after dropping caches: with every chunk filled, how many events now?
    sh("sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches")
    t = time.time()
    got2, _, _ = hash_tree(mnt)
    rec["reread_s"] = time.time() - t
    rec["reread_content"] = compare_hashes(ref, got2)
    rec["reread_events"] = call(op="stats")["counters"]["events"]
    umount(mnt)
    call(op="quit") if False else None
    p.kill()
    p.wait()
    out[label] = rec
    print(label, json.dumps(rec)[:900], flush=True)
json.dump(out, open("/data/results/mdsmoke.json", "w"), indent=1)
