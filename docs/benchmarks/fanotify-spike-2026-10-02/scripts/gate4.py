#!/usr/bin/env python3
"""S13 gate 4: disk sharing across images on one node.

  gate4.py align                       FICLONERANGE alignment rules on XFS (reflink=1)
  gate4.py run --mode copy|reflink|multidev [--n 24]

Each run hydrates the first N images of S12's 64-image page-cache set (4 shared bases) one after
another on a fresh XFS (reflink=1) under /xfs: attach through fand.py, mount file-backed, read every
file (sha256), unmount, keep the backing files. After each image: sync and the filesystem's used-bytes
delta, fill counters and wall time. Modes:
  copy      per-image unified sparse files, filled with pwrite (gate 3's path);
  reflink   the same files, filled with FICLONERANGE from a shared chunk cache (one file per chunk
            id, verified once when written) on the same XFS;
  multidev  per-image bootstrap file + one sparse file per blob (layer), shared by every image that
            has it, mounted with -o device= (EROFS multi-device in file-backed mode).
File hashes are compared across modes (the copy run is the reference).
"""
import argparse
import fcntl
import hashlib
import json
import os
import shutil
import struct
import subprocess
import sys
import time

sys.path.insert(0, "/root/s13")
sys.path.insert(0, "/root/s12")
import coldfan as F  # noqa: E402
from common import drop_caches, hash_tree, sh  # noqa: E402

XFS = "/xfs"
RES = "/data/results"
PC = json.load(open("/data/manifests/pc-manifests.json"))
FICLONERANGE = 0x4020940D


def used():
    os.sync()
    s = os.statvfs(XFS)
    return (s.f_blocks - s.f_bfree) * s.f_frsize


def fresh_xfs():
    sh("umount", XFS, check=False)
    sh("truncate", "-s", "0", "/data/xfs.img")
    sh("truncate", "-s", "400G", "/data/xfs.img")
    r = sh("mkfs.xfs", "-f", "-q", "-m", "reflink=1", "/data/xfs.img")
    os.makedirs(XFS, exist_ok=True)
    sh("mount", "-o", "loop", "/data/xfs.img", XFS)
    return sh("xfs_info", XFS).stdout


def align(a):
    info = fresh_xfs()
    out = {"xfs_info": info, "cases": []}
    src = f"{XFS}/slots.bin"
    data = os.urandom(256 << 10)
    with open(src, "wb") as f:
        for k in range(4):
            f.write(data)
    with open(f"{XFS}/onechunk.bin", "wb") as f:
        f.write(data[:100000])  # 100,000 B: not a 4 KiB multiple, at EOF
    with open(f"{XFS}/onechunk-padded.bin", "wb") as f:
        f.write(data[:100000] + b"\0" * 2400)  # padded to 102,400 B, a 4 KiB multiple
    dst = f"{XFS}/dst.bin"
    with open(dst, "wb") as f:
        f.truncate(4 << 20)
    cases = [("slot 256K -> 4K-aligned dest, len 256K", src, 256 << 10, 256 << 10, 8192),
             ("slot 256K -> dest 1M, len 100000 (unaligned, not at src EOF)", src, 256 << 10, 100000, 1 << 20),
             ("slot 256K -> dest 1M, len round_up(100000, 4K)", src, 256 << 10, 102400, 1 << 20),
             ("per-chunk file, len 0 (to EOF, 100000 B) -> dest 2M", f"{XFS}/onechunk.bin", 0, 0, 2 << 20),
             ("per-chunk file, len 100000 (EOF) -> dest 2M+4K", f"{XFS}/onechunk.bin", 0, 100000, (2 << 20) + 4096),
             ("dest offset not 4K-aligned (dest 2M+512)", src, 0, 4096, (2 << 20) + 512),
             ("per-chunk file padded to 4K, len 102400 -> dest 3M", f"{XFS}/onechunk-padded.bin", 0, 102400, 3 << 20),
             ("per-chunk file, len 0 (to EOF) -> dest at its EOF (dest 4M)", f"{XFS}/onechunk.bin", 0, 0, 4 << 20)]
    dfd = os.open(dst, os.O_RDWR)
    for name, s, soff, ln, doff in cases:
        sfd = os.open(s, os.O_RDONLY)
        t = time.perf_counter()
        try:
            fcntl.ioctl(dfd, FICLONERANGE, struct.pack("<qQQQ", sfd, soff, ln, doff))
            r = "ok"
            n = ln or os.fstat(sfd).st_size
            ok = os.pread(dfd, n, doff) == os.pread(sfd, n, soff)
        except OSError as e:
            r, ok = f"errno {e.errno} {e.strerror}", None
        out["cases"].append({"case": name, "result": r, "content_equal": ok, "us": (time.perf_counter() - t) * 1e6})
        os.close(sfd)
    os.close(dfd)
    json.dump(out, open(f"{RES}/gate4-align.json", "w"), indent=1)
    print(json.dumps(out["cases"], indent=1))


def run(a):
    xinfo = fresh_xfs()
    extra = []
    if a.mode == "reflink":
        extra = ["--fill", "reflink", "--chunk-cache", f"{XFS}/cc"]
    F.BF = f"{XFS}/bf"
    be = F.FanBackend(extra)
    steps = []
    u0 = used()
    ref = {}
    if os.path.exists(f"{RES}/gate4-hashes-copy.json") and a.mode != "copy":
        ref = json.load(open(f"{RES}/gate4-hashes-copy.json"))
    hashes = {}
    for k in range(a.n):
        name = f"pc-{k:03d}"
        drop_caches()
        be.call(op="stats", reset=True)
        t0 = time.time()
        mnt = f"/mnt/s13g4/{name}"
        os.makedirs(mnt, exist_ok=True)
        if a.mode == "multidev":
            r = be.call(op="attach_multidev", name=name, dir=f"{XFS}/md")
            if not r.get("ok"):
                raise RuntimeError(r)
            opts = "ro," + ",".join(f"device={d}" for d in r["devices"])
            src = r["boot"]
        else:
            r = be.call(op="attach_image", name=name, path=f"{XFS}/bf/{name}.img")
            if not r.get("ok"):
                raise RuntimeError(r)
            opts, src = "ro", r["path"]
        m = sh("mount", "-t", "erofs", "-o", opts, src, mnt, check=False)
        rec = {"k": k + 1, "name": name, "family": PC[k]["family"], "base": PC[k]["manifest"]["layers"][0]["digest"][:19],
               "mount_rc": m.returncode, "mount_err": m.stderr[-200:], "attach": r}
        if m.returncode == 0:
            t1 = time.time()
            h, errors, nbytes = hash_tree(mnt)
            rec["read_s"] = time.time() - t1
            rec["files"], rec["bytes"], rec["read_errors"] = len(h), nbytes, len(errors)
            hashes[name] = h
            if ref.get(name):
                rec["mismatch_vs_copy"] = sum(1 for p in ref[name] if h.get(p) != ref[name][p])
            sh("umount", mnt, check=False)
        st = be.call(op="stats")
        rec["wall_s"] = time.time() - t0
        rec["counters"] = st["counters"]
        rec["event_ms"] = st["event_ms"]
        u = used()
        rec["fs_used_delta_bytes"] = u - u0
        u0 = u
        steps.append(rec)
        print(json.dumps({x: rec.get(x) for x in ("k", "name", "family", "mount_rc", "read_s", "bytes",
                                                   "fs_used_delta_bytes", "mismatch_vs_copy")}),
              "fill", rec["counters"]["fill_bytes"], "clone", rec["counters"]["clone_chunks"],
              "cachew", rec["counters"]["cache_file_writes"], flush=True)
        json.dump({"mode": a.mode, "xfs_info": xinfo, "steps": steps}, open(f"{RES}/gate4-{a.mode}.json", "w"), indent=1)
    be.close()
    if a.mode == "copy":
        json.dump(hashes, open(f"{RES}/gate4-hashes-copy.json", "w"))
    deltas = [s["fs_used_delta_bytes"] for s in steps[1:]]
    summary = {"mode": a.mode, "n": len(steps), "first_image_bytes": steps[0]["fs_used_delta_bytes"],
               "per_extra_image_mean_bytes": sum(deltas) / len(deltas) if deltas else None,
               "per_extra_image_median_bytes": sorted(deltas)[len(deltas) // 2] if deltas else None,
               "total_bytes": sum(s["fs_used_delta_bytes"] for s in steps),
               "image_bytes_read": sum(s.get("bytes", 0) for s in steps),
               "read_s_median": sorted(s.get("read_s", 0) for s in steps)[len(steps) // 2],
               "mismatches": sum(s.get("mismatch_vs_copy", 0) or 0 for s in steps),
               "mount_failures": sum(1 for s in steps if s["mount_rc"])}
    print(json.dumps(summary), flush=True)
    json.dump({"mode": a.mode, "summary": summary, "xfs_info": xinfo, "steps": steps},
              open(f"{RES}/gate4-{a.mode}.json", "w"), indent=1)
    sh("umount", XFS, check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["align", "run"])
    ap.add_argument("--mode", default="copy")
    ap.add_argument("--n", type=int, default=24)
    a = ap.parse_args()
    align(a) if a.what == "align" else run(a)


main()
