#!/usr/bin/env python3
"""S13: build the unified address space of a RAFS v6 image as one file.

  flatten.py BOOTSTRAP BLOBDIR OUT [--sparse SPARSE]

OUT is complete: the bootstrap at offset 0 and each blob's chunks, decompressed and verified, at
mapped_blkaddr * 4096 + the chunk's uncompressed offset (S10's unified device). SPARSE gets only the
bootstrap and the same size, so everything else is a hole for the listener to fill.
"""
import argparse
import hashlib
import json
import os
import sys
from compression import zstd

sys.path.insert(0, "/root/s12")
import rafs  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("boot")
ap.add_argument("blobs")
ap.add_argument("out")
ap.add_argument("--sparse")
a = ap.parse_args()
info = rafs.read(a.boot)
boot = open(a.boot, "rb").read()
base = [d["mapped_blkaddr"] * 4096 for d in info["devices"]]
size = max([len(boot)] + [b + d["blocks"] * 4096 for b, d in zip(base, info["devices"])])
size = (size + 4095) // 4096 * 4096
fds = [os.open(f"{a.blobs}/{d['blob_id']}", os.O_RDONLY) for d in info["devices"]]
out = os.open(a.out, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
os.ftruncate(out, size)
os.pwrite(out, boot, 0)
n = 0
for (dg, bi, fl, cs, us, co, uo) in info["chunks"]:
    raw = os.pread(fds[bi], cs, co)
    data = zstd.decompress(raw) if fl & 1 else raw
    assert len(data) == us and hashlib.sha256(data).hexdigest() == dg, dg
    os.pwrite(out, data, base[bi] + uo)
    n += 1
os.close(out)
if a.sparse:
    s = os.open(a.sparse, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
    os.ftruncate(s, size)
    os.pwrite(s, boot, 0)
    os.close(s)
print(json.dumps({"size": size, "bootstrap": len(boot), "chunks": n, "devices": info["devices"]}))
