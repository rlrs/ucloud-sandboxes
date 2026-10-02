#!/usr/bin/env python3
"""S13 gate 1: do EROFS's own reads of a file-backed image raise FAN_PRE_ACCESS on the backing file?

  gate1.py [--idx 130]

Images (from the S12/S10 conversion in /data/work/nodict-256k):
  rafs     one RAFS v6 image (nydus-image v2.4.5) as one unified file: bootstrap at 0, blobs at
           mapped_blkaddr (flatten.py). The sparse copy holds only the bootstrap.
  plain    mkfs.erofs 1.9 of the same tree, one file with metadata and data interleaved.
  blobdev  mkfs.erofs 1.9 --chunksize 256 KiB --blobdev: metadata file + data blob, mounted with
           -o device= (multi-device in file-backed mode). The blob is sparse.
Tests, each on a fresh sparse copy, fresh mount and dropped caches, with fanl.py filling from the
complete image: read() of every file, mmap page faults on the largest files, directio, no listener,
and an mmap of the marked backing file itself.
"""
import argparse
import hashlib
import json
import mmap
import os
import sys
import time

sys.path.insert(0, "/root/s13")
from common import (Listener, compare_hashes, dmesg_tail, drop_caches, fresh_copy, hash_tree, mount_erofs,  # noqa: E402
                    sh, summarize_events, umount)

G = "/data/s13/g1"
RUN = "/data/work/nodict-256k"
OUT = "/data/results/gate1.json"
RES = {}


def save():
    json.dump(RES, open(OUT, "w"), indent=1)


def largest_files(root, n=12):
    sizes = []
    for dp, dn, fn in os.walk(root):
        for f in fn:
            p = os.path.join(dp, f)
            if os.path.isfile(p) and not os.path.islink(p):
                sizes.append((os.path.getsize(p), p[len(root):]))
    return [p for _, p in sorted(sizes, reverse=True)[:n]]


def mmap_hash(root, rels):
    out, faults = {}, []
    for rel in rels:
        p = root + rel
        with open(p, "rb") as f:
            size = os.fstat(f.fileno()).st_size
            if size == 0:
                continue
            m = mmap.mmap(f.fileno(), size, prot=mmap.PROT_READ)
            h = hashlib.sha256()
            for off in range(0, size, 1 << 20):  # touch through the mapping, 1 MiB at a time
                h.update(m[off:off + (1 << 20)])
            m.close()
            out[rel] = h.hexdigest()
    return out


def run_case(name, backing, source, mnt, opts="ro", mode="read", files=None, ref=None, listener=True, extra_marks=()):
    drop_caches()
    log = f"{G}/ev-{name}.jsonl"
    lst = None
    if listener:
        marks = [f"{backing}={source}"] + list(extra_marks)
        args = [x for m in marks for x in ("--mark", m)]
        lst = Listener(*args, log=log)
    mo = mount_erofs(opts[1] if isinstance(opts, tuple) else backing, mnt, opts[0] if isinstance(opts, tuple) else opts)
    rec = {"case": name, "mount": mo}
    if mo["rc"] == 0:
        t = time.perf_counter()
        try:
            if mode == "read":
                got, errors, nbytes = hash_tree(mnt)
                rec["read_errors"] = len(errors)
                rec["read_error_examples"] = list(errors.items())[:3]
            else:
                got = mmap_hash(mnt, files)
                nbytes = sum(os.path.getsize(mnt + f) for f in got)
        except OSError as e:
            got, nbytes = {}, 0
            rec["exception"] = repr(e)
        rec["wall_s"] = time.perf_counter() - t
        rec["bytes"] = nbytes
        if ref is not None:
            rec["content"] = compare_hashes({k: ref[k] for k in (files or ref) if k in ref}, got)
        umount(mnt)
    else:
        got = {}
    rec["dmesg"] = dmesg_tail(6)
    if lst:
        lst.kill(15)
        evs = lst.events()
        rec["events"] = summarize_events(evs)
        rec["first_events"] = evs[:4]
    RES[name] = rec
    save()
    print(name, json.dumps({k: rec.get(k) for k in ("mount", "content", "wall_s")}),
          json.dumps(rec.get("events", {}))[:600], flush=True)
    return got


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--idx", type=int, default=130)
    a = ap.parse_args()
    os.makedirs(G, exist_ok=True)
    boot = f"{RUN}/images/{a.idx:03d}.boot"
    fl = json.loads(sh("python3", "/root/s13/flatten.py", boot, f"{RUN}/blobs", f"{G}/rafs-full.img",
                       "--sparse", f"{G}/rafs-sparse.img").stdout)
    RES["image"] = {"idx": a.idx, "flatten": fl, "uname": os.uname().release}
    mnt = "/mnt/s13g1"
    # Reference: the complete unified RAFS file, file-backed, no listener.
    drop_caches()
    mo = mount_erofs(f"{G}/rafs-full.img", mnt)
    RES["rafs_full_mount"] = mo
    ref, errs, nbytes = hash_tree(mnt)
    RES["reference"] = {"files": len(ref), "bytes": nbytes, "errors": len(errs)}
    big = largest_files(mnt)
    RES["mmap_files"] = big
    # Plain and blobdev mkfs.erofs images built from the same tree.
    t = time.time()
    r1 = sh("mkfs.erofs", "--quiet", f"{G}/plain-full.img", mnt, check=False)
    r2 = sh("mkfs.erofs", "--quiet", "--chunksize=262144", f"--blobdev={G}/blob-full.img", f"{G}/meta.img", mnt,
            check=False)
    RES["mkfs"] = {"plain_rc": r1.returncode, "plain_err": r1.stderr[-300:], "blobdev_rc": r2.returncode,
                   "blobdev_err": r2.stderr[-300:], "s": time.time() - t,
                   "plain_bytes": os.path.getsize(f"{G}/plain-full.img") if r1.returncode == 0 else 0,
                   "meta_bytes": os.path.getsize(f"{G}/meta.img") if r2.returncode == 0 else 0,
                   "blob_bytes": os.path.getsize(f"{G}/blob-full.img") if r2.returncode == 0 else 0}
    umount(mnt)
    save()

    sp = f"{G}/rafs-sparse.img"
    w = f"{G}/work.img"
    # 1. RAFS unified sparse file: read() of every file.
    fresh_copy(sp, w)
    run_case("rafs_read", w, f"{G}/rafs-full.img", mnt, ref=ref)
    # 2. RAFS: mmap page faults only (no read() of those files first).
    fresh_copy(sp, w)
    run_case("rafs_mmap", w, f"{G}/rafs-full.img", mnt, mode="mmap", files=big, ref=ref)
    # 3. RAFS with -o directio.
    fresh_copy(sp, w)
    run_case("rafs_read_directio", w, f"{G}/rafs-full.img", mnt, opts="ro,directio", ref=ref)
    # 4. No listener: what unfilled holes read as.
    fresh_copy(sp, w)
    run_case("rafs_no_listener", w, f"{G}/rafs-full.img", mnt, ref=ref, listener=False)
    # 5. Plain mkfs.erofs image, completely sparse (nothing prefilled).
    if r1.returncode == 0:
        sh("truncate", "-s", str(os.path.getsize(f"{G}/plain-full.img")), f"{G}/plain-empty.img")
        fresh_copy(f"{G}/plain-empty.img", w)
        run_case("plain_empty", w, f"{G}/plain-full.img", mnt, ref=ref)
        # 5b. Superblock block prefilled only.
        fresh_copy(f"{G}/plain-empty.img", w)
        with open(f"{G}/plain-full.img", "rb") as s, open(w, "r+b") as d:
            d.write(s.read(4096))
        run_case("plain_superblock_only", w, f"{G}/plain-full.img", mnt, ref=ref)
        # 5c. Complete plain image under a listener (does data I/O raise events when nothing is missing?).
        fresh_copy(f"{G}/plain-full.img", w)
        run_case("plain_full_listener", w, f"{G}/plain-full.img", mnt, ref=ref)
    # 6. mkfs.erofs --blobdev: complete metadata file, sparse data blob, -o device=.
    if r2.returncode == 0:
        sh("truncate", "-s", str(os.path.getsize(f"{G}/blob-full.img")), f"{G}/blob-empty.img")
        fresh_copy(f"{G}/blob-empty.img", w)
        run_case("blobdev_read", w, f"{G}/blob-full.img", mnt, opts=("ro,device=" + w, f"{G}/meta.img"), ref=ref)
        fresh_copy(f"{G}/blob-empty.img", w)
        run_case("blobdev_mmap", w, f"{G}/blob-full.img", mnt, opts=("ro,device=" + w, f"{G}/meta.img"),
                 mode="mmap", files=big, ref=ref)
    # 8. Mark placed only after the mount opened the backing file.
    fresh_copy(sp, w)
    drop_caches()
    mo = mount_erofs(w, mnt)
    lst = Listener("--mark", f"{w}={G}/rafs-full.img", log=f"{G}/ev-mark_after_mount.jsonl")
    got, errors, _ = hash_tree(mnt)
    umount(mnt)
    lst.kill(15)
    RES["mark_after_mount"] = {"mount": mo, "content": compare_hashes(ref, got), "events": summarize_events(lst.events())}
    save()
    print("mark_after_mount", json.dumps(RES["mark_after_mount"])[:500], flush=True)
    # 7. mmap of the marked backing file itself, outside EROFS: event at mmap() or at fault?
    fresh_copy(sp, w)
    log = f"{G}/ev-backing_mmap.jsonl"
    lst = Listener("--mark", f"{w}={G}/rafs-full.img", log=log)
    fd = os.open(w, os.O_RDONLY)
    size = os.fstat(fd).st_size
    t0 = time.time()
    m = mmap.mmap(fd, min(size, 64 << 20), prot=mmap.PROT_READ)
    time.sleep(0.5)
    n_after_mmap = len(lst.events())
    _ = m[(32 << 20) % len(m)]
    time.sleep(0.5)
    n_after_fault = len(lst.events())
    m.close()
    os.close(fd)
    lst.kill(15)
    evs = lst.events()
    RES["backing_mmap"] = {"events_after_mmap": n_after_mmap, "events_after_fault": n_after_fault,
                           "events": evs[:4], "t0": t0}
    save()
    print("backing_mmap", json.dumps(RES["backing_mmap"])[:500], flush=True)
    print("GATE1_DONE", flush=True)


main()
