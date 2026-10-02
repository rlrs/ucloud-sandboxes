#!/usr/bin/env python3
"""S12 (e): host page-cache and memory cost per extra distinct image, by mount granularity.

  pagecache.py prep                 mount every candidate's RAFS image (local packs), keep those with
                                    python3, count OCI whiteout names per layer, and build a flattened
                                    EROFS image per image with mkfs.erofs 1.9 --xattr-inode-digest
                                    (the fingerprint inode_share needs), for variant ishare
  pagecache.py run --variant image|layer|ishare|erofs [--n 60]

Variants (each starts from torn-down mounts and dropped caches; images attach one after another and
run the same import command once in runsc, then stay mounted):
  image   one merged RAFS bootstrap per image, one NBD device each (design §4)
  layer   one RAFS mount per OCI layer, shared by every image that has the layer, stacked with
          OverlayFS per image (today's composition granularity)
  ishare  one mkfs.erofs image per image on a read-only direct-I/O loop device, mounted with
          -o inode_share,domain_id=s12 (needs erofs.ko with EROFS_FS_PAGE_CACHE_SHARE)
  erofs   the same mkfs.erofs images without inode_share: the control for ishare
The NBD backend reads packs from local disk with POSIX_FADV_DONTNEED and keeps no chunk cache, so the
page cache that grows is the kernel's own (EROFS file pages and device buffers).
"""
import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/root/s12")
import coldrun as C  # noqa: E402  (reuses Backend, runsc, drop_caches)

PC = json.load(open("/data/manifests/pc-manifests.json"))
OUT = "/data/pc-erofs"
CMD = ["sh", "-c", "python3 -c 'import json, sqlite3, ssl, unittest, email, http.client, asyncio, decimal, argparse, logging'"]
MEMKEYS = ("MemAvailable", "Cached", "Buffers", "SReclaimable", "Active(file)", "Inactive(file)", "Shmem", "Mapped")


def meminfo():
    d = {}
    for line in open("/proc/meminfo"):
        k, v = line.split(":")
        if k in MEMKEYS:
            d[k] = int(v.split()[0]) * 1024
    return d


def rss(pid):
    for line in open(f"/proc/{pid}/status"):
        if line.startswith("VmRSS"):
            return int(line.split()[1]) * 1024
    return 0


def cfg(i):
    return json.load(open(f"/data/oci/blobs/sha256/{PC[i]['manifest']['config']['digest'].split(':')[1]}")).get("config", {})


class Mounts:
    def __init__(self, be=None):
        self.be, self.mounts, self.devs, self.loops = be, [], [], []

    def nbd(self, name):
        dev = C.next_dev()
        r = self.be.call(op="attach", dev=dev, name=name, mode="demand")
        if not r.get("ok"):
            raise RuntimeError(r)
        self.devs.append(dev)
        return dev

    def mount(self, *args):
        os.makedirs(args[-1], exist_ok=True)
        r = C.sh("mount", *args, check=False)
        if r.returncode:
            raise RuntimeError(f"mount {args}: {r.stderr}")
        self.mounts.append(args[-1])

    def overlay(self, base, lowers):
        for d in ("upper", "work", "rootfs"):
            os.makedirs(f"{base}/{d}", exist_ok=True)
        self.mount("-t", "overlay", "overlay", "-o",
                   f"lowerdir={':'.join(lowers)},upperdir={base}/upper,workdir={base}/work", f"{base}/rootfs")
        return f"{base}/rootfs"

    def loop(self, path):
        dev = C.sh("losetup", "--direct-io=on", "-r", "-f", "--show", path).stdout.strip()
        self.loops.append(dev)
        return dev

    def close(self):
        for m in reversed(self.mounts):
            C.sh("umount", m, check=False)
        for d in self.devs:
            self.be.call(op="detach", dev=d)
        for l in self.loops:
            C.sh("losetup", "-d", l, check=False)


def prep(a):
    os.makedirs(OUT, exist_ok=True)
    be = C.Backend("local", ["--mem-bytes", "0", "--no-disk-cache", "--fadvise"])
    m = Mounts(be)
    info = []

    def one(i):
        base = f"/data/pcm/prep-{i}"
        dev = m.nbd(f"pc-{i:03d}")
        m.mount("-t", "erofs", "-o", "ro", dev, f"{base}/lower")
        lower = f"{base}/lower"
        py = any(os.path.exists(lower + p) for p in ("/usr/bin/python3", "/usr/local/bin/python3"))
        rec = {"idx": i, "family": PC[i]["family"], "python3": py, "layers": len(PC[i]["manifest"]["layers"])}
        if py:
            t0 = time.time()
            r = C.sh("mkfs.erofs", "--quiet", "--xattr-inode-digest=trusted.erofs.fingerprint", "--preserve-mtime",
                     f"{OUT}/pc-{i:03d}.erofs", lower, check=False)
            rec.update(mkfs_rc=r.returncode, mkfs_s=time.time() - t0, mkfs_err=r.stderr[-300:],
                       erofs_bytes=os.path.getsize(f"{OUT}/pc-{i:03d}.erofs") if r.returncode == 0 else 0)
            # Sample files for the content check (path, sha256 through the RAFS mount).
            files = []
            for dp, dn, fn in os.walk(lower):
                for f in fn:
                    p = os.path.join(dp, f)
                    if os.path.isfile(p) and not os.path.islink(p):
                        files.append(p)
            random.Random(i).shuffle(files)
            rec["sample"] = [(p[len(lower):], hashlib.sha256(open(p, "rb").read()).hexdigest()) for p in files[:60]]
            rec["files"] = len(files)
        return rec
    with ThreadPoolExecutor(8) as ex:
        info = list(ex.map(one, range(len(PC))))
    # Whiteouts in per-layer RAFS mounts: OCI ".wh." names (OverlayFS ignores) vs char 0:0 devices.
    wh = {}
    for i in range(len(PC)):
        for l in PC[i]["manifest"]["layers"]:
            h = l["digest"].split(":")[1]
            if h in wh or not os.path.exists(f"/data/s12/maps/layer-{h}.map.zst"):
                continue
            dev = m.nbd(f"layer-{h}")
            mp = f"/data/pcm/prep-layer-{h[:12]}"
            m.mount("-t", "erofs", "-o", "ro", dev, mp)
            n_wh = n_cdev = n_opq = 0
            for dp, dn, fn in os.walk(mp):
                for f in fn + dn:
                    p = os.path.join(dp, f)
                    if f.startswith(".wh."):
                        n_wh += 1
                    st = os.lstat(p)
                    if (st.st_mode & 0o170000) == 0o020000 and st.st_rdev == 0:
                        n_cdev += 1
                for d in dn:
                    try:
                        if os.getxattr(os.path.join(dp, d), "trusted.overlay.opaque", follow_symlinks=False):
                            n_opq += 1
                    except OSError:
                        pass
            wh[h] = {"wh_names": n_wh, "char00": n_cdev, "opaque_dirs": n_opq}
    m.close()
    be.close()
    json.dump({"images": info, "layer_whiteouts": wh}, open("/data/results/pc-prep.json", "w"), indent=1)
    ok = [r for r in info if r.get("python3") and r.get("mkfs_rc") == 0]
    print(json.dumps({"candidates": len(info), "with_python_and_erofs": len(ok),
                      "layers_with_wh_names": sum(1 for v in wh.values() if v["wh_names"]),
                      "layers_with_char00": sum(1 for v in wh.values() if v["char00"])}), flush=True)


def run(a):
    prep_info = json.load(open("/data/results/pc-prep.json"))["images"]
    imgs = [r["idx"] for r in prep_info if r.get("python3") and r.get("mkfs_rc") == 0][: a.n]
    shutil.rmtree("/data/pcm", ignore_errors=True)
    be = None
    if a.variant in ("image", "layer"):
        be = C.Backend("local", ["--mem-bytes", "0", "--no-disk-cache", "--fadvise"])
    m = Mounts(be)
    layer_mounts = {}
    time.sleep(1)
    C.drop_caches()
    time.sleep(2)
    m0 = meminfo()
    rss0 = rss(be.p.pid) if be else 0
    steps = []
    for k, i in enumerate(imgs):
        base = f"/data/pcm/{a.variant}-{i}"
        t0 = time.time()
        new_layers = 0
        if a.variant == "image":
            dev = m.nbd(f"pc-{i:03d}")
            m.mount("-t", "erofs", "-o", "ro", dev, f"{base}/lower")
            root = m.overlay(base, [f"{base}/lower"])
        elif a.variant == "layer":
            lowers = []
            for l in PC[i]["manifest"]["layers"]:
                h = l["digest"].split(":")[1]
                if h not in layer_mounts:
                    dev = m.nbd(f"layer-{h}")
                    mp = f"/data/pcm/layers/{h[:16]}"
                    m.mount("-t", "erofs", "-o", "ro", dev, mp)
                    layer_mounts[h] = mp
                    new_layers += 1
                lowers.append(layer_mounts[h])
            root = m.overlay(base, list(reversed(lowers)))
        else:
            dev = m.loop(f"{OUT}/pc-{i:03d}.erofs")
            opts = "ro,inode_share,domain_id=s12" if a.variant == "ishare" else "ro"
            m.mount("-t", "erofs", "-o", opts, dev, f"{base}/lower")
            root = m.overlay(base, [f"{base}/lower"])
        attach = time.time() - t0
        r = C.runsc(root, cfg(i), CMD, f"s12pc-{a.variant}-{k}")
        mi = meminfo()
        step = {"k": k + 1, "idx": i, "family": PC[i]["family"], "attach_s": attach, "new_layer_mounts": new_layers,
                "cmd_s": r["wall"], "rc": r["rc"], "out": r["out"][-120:] if r["rc"] else "",
                "mem": {x: mi[x] - m0[x] for x in MEMKEYS},
                "backend_rss_delta": (rss(be.p.pid) - rss0) if be else 0}
        steps.append(step)
        print(json.dumps({x: step[x] for x in ("k", "idx", "attach_s", "cmd_s", "rc", "new_layer_mounts")}),
              "cachedMB", round(step["mem"]["Cached"] / 2 ** 20, 1), "availMB", round(-step["mem"]["MemAvailable"] / 2 ** 20, 1),
              "buffersMB", round(step["mem"]["Buffers"] / 2 ** 20, 1), flush=True)
    check = None
    if a.variant in ("ishare", "erofs"):
        # Content check: sampled files through the ishare/erofs mount equal the RAFS tree's sha256.
        check = {"files": 0, "mismatch": 0, "missing": 0}
        for r in prep_info:
            if r["idx"] not in imgs:
                continue
            lower = f"/data/pcm/{a.variant}-{r['idx']}/lower"
            for p, h in r["sample"]:
                check["files"] += 1
                try:
                    check["mismatch"] += hashlib.sha256(open(lower + p, "rb").read()).hexdigest() != h
                except OSError:
                    check["missing"] += 1
        print("content", json.dumps(check), flush=True)
    dmesg = C.sh("dmesg", check=False).stdout.splitlines()[-30:]
    m.close()
    if be:
        be.close()
    # Cost per extra distinct image: least-squares slope over images 2..n.
    def slope(key, sign=1):
        xs = [s["k"] for s in steps[1:]]
        ys = [sign * s["mem"][key] for s in steps[1:]]
        if len(xs) < 2:
            return None
        mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
        return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    summary = {"variant": a.variant, "n": len(steps), "failures": sum(1 for s in steps if s["rc"]),
               "first_image_cached_MB": steps[0]["mem"]["Cached"] / 2 ** 20 if steps else None,
               "per_extra_image_cached_MB": (slope("Cached") or 0) / 2 ** 20,
               "per_extra_image_buffers_MB": (slope("Buffers") or 0) / 2 ** 20,
               "per_extra_image_used_MB": (slope("MemAvailable", -1) or 0) / 2 ** 20,
               "per_extra_image_slab_MB": (slope("SReclaimable") or 0) / 2 ** 20,
               "total_cached_MB": steps[-1]["mem"]["Cached"] / 2 ** 20 if steps else None,
               "total_used_MB": -steps[-1]["mem"]["MemAvailable"] / 2 ** 20 if steps else None,
               "attach_median_s": sorted(s["attach_s"] for s in steps)[len(steps) // 2] if steps else None,
               "cmd_median_s": sorted(s["cmd_s"] for s in steps)[len(steps) // 2] if steps else None,
               "layer_mounts": len(layer_mounts), "content_check": check}
    print(json.dumps(summary), flush=True)
    json.dump({"summary": summary, "steps": steps, "dmesg_tail": dmesg},
              open(f"/data/results/pagecache-{a.variant}.json", "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["prep", "run"])
    ap.add_argument("--variant", default="image")
    ap.add_argument("--n", type=int, default=60)
    a = ap.parse_args()
    prep(a) if a.what == "prep" else run(a)


if __name__ == "__main__":
    main()
