#!/usr/bin/env python3
"""S13 gate 2 control: the same death test on today's path. S12's NBD backend (local packs) serves
img-130; a reader of its largest file runs while the backend is killed (SIGKILL). Then a new read."""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, "/root/s12")
sys.path.insert(0, "/root/s13")
import coldrun as C  # noqa: E402
from common import drop_caches, hash_tree, mount_erofs, umount  # noqa: E402

READER = r"""
import hashlib, json, sys, time
t = time.time()
try:
    d = open(sys.argv[1], 'rb').read()
    print(json.dumps({"sha": hashlib.sha256(d).hexdigest(), "zeros": d.count(0) == len(d), "s": time.time() - t}))
except OSError as e:
    print(json.dumps({"errno": e.errno, "error": e.strerror, "s": time.time() - t}))
"""
mnt = "/mnt/s13nbdd"
assert mount_erofs("/data/s13/g1/rafs-full.img", mnt)["rc"] == 0
ref, _, _ = hash_tree(mnt)
umount(mnt)
sizes = {}
assert mount_erofs("/data/s13/g1/rafs-full.img", mnt)["rc"] == 0
for p in ref:
    sizes[p] = os.path.getsize(mnt + p)
umount(mnt)
files = sorted(sizes, key=lambda p: -sizes[p])[:3]
be = C.Backend("local")
drop_caches()
dev = "/dev/nbd900"
r = be.call(op="attach", dev=dev, name="img-130", mode="demand")
assert mount_erofs(dev, mnt)["rc"] == 0
rd = subprocess.Popen(["python3", "-c", READER, mnt + files[0]], stdout=subprocess.PIPE, text=True)
time.sleep(0.05)
os.kill(be.p.pid, 9)
t = time.time()
try:
    out1 = rd.communicate(timeout=180)[0]
except subprocess.TimeoutExpired:
    out1 = json.dumps({"blocked_after_s": 180})
res1 = json.loads(out1)
res1["correct"] = res1.get("sha") == ref[files[0]]
res1["after_kill_s"] = time.time() - t
rd2 = subprocess.run(["python3", "-c", READER, mnt + files[1]], capture_output=True, text=True, timeout=200)
res2 = json.loads(rd2.stdout)
res2["correct"] = res2.get("sha") == ref[files[1]]
dm = C.sh("dmesg", check=False).stdout.splitlines()[-6:]
umount(mnt)
out = {"reader_during_kill": res1, "new_read_after_kill": res2, "dmesg": dm,
       "nbd_timeout_s": 120, "note": "s3nbd sets NBD_SET_TIMEOUT 120"}
print(json.dumps(out, indent=1))
json.dump(out, open("/data/results/nbd-death.json", "w"), indent=1)
