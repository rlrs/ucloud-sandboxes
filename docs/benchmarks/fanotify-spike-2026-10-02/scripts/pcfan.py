#!/usr/bin/env python3
"""S13 gate 5: host memory per extra distinct image by mount granularity (S12's lost item e),
on file-backed EROFS.

  pcfan.py prep                         per pc image: mount (fand, per-image), check python3, build a
                                        fingerprinted mkfs.erofs 1.9 image (--xattr-inode-digest) and
                                        sample 60 file hashes; dump a few fingerprint xattrs
  pcfan.py run --variant V [--n 64]

Variants (each from no mounts and dropped caches; images attach one after another, run S12's
import-heavy command once in runsc, and stay mounted):
  image[-dio]     one merged RAFS image per image, a sparse unified file filled by fand.py; -dio mounts
                  with -o directio and fills with O_DIRECT, so the backing file is never cached
  layer[-dio]     one RAFS image per OCI layer (fand.py), mounted once and shared by every image that
                  has the layer, stacked per image with OverlayFS (today's composition granularity)
  multidev        per-image RAFS bootstrap + per-blob sparse files shared across images (-o device=)
  erofs[-dio]     the fingerprinted mkfs.erofs images, complete files, file-backed, no inode_share
  ishare[-dio]    the same with -o inode_share,domain_id=s13 (erofs.ko with EROFS_FS_PAGE_CACHE_SHARE)
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

sys.path.insert(0, "/root/s13")
sys.path.insert(0, "/root/s12")
import coldfan as F  # noqa: E402
import coldrun as C  # noqa: E402
from common import drop_caches, sh  # noqa: E402

PC = json.load(open("/data/manifests/pc-manifests.json"))
OUT = "/data/s13/pce"
RES = "/data/results"
CMD = ["sh", "-c", "python3 -c 'import json, sqlite3, ssl, unittest, email, http.client, asyncio, decimal, argparse, logging'"]
MEMKEYS = ("MemAvailable", "MemFree", "Cached", "Buffers", "SReclaimable", "SUnreclaim", "Active(file)",
           "Inactive(file)", "Shmem", "Mapped")
FP_NAME = "trusted.erofs.fingerprint"


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
    def __init__(self):
        self.mounts = []

    def mount(self, *args):
        os.makedirs(args[-1], exist_ok=True)
        r = sh("mount", *args, check=False)
        if r.returncode:
            raise RuntimeError(f"mount {args}: {r.stderr}")
        self.mounts.append(args[-1])

    def overlay(self, base, lowers):
        for d in ("upper", "work", "rootfs"):
            os.makedirs(f"{base}/{d}", exist_ok=True)
        self.mount("-t", "overlay", "overlay", "-o",
                   f"lowerdir={':'.join(lowers)},upperdir={base}/upper,workdir={base}/work", f"{base}/rootfs")
        return f"{base}/rootfs"

    def close(self):
        for m in reversed(self.mounts):
            sh("umount", m, check=False)


def prep(a):
    os.makedirs(OUT, exist_ok=True)
    F.BF = "/data/s13/pcbf"
    be = F.FanBackend()
    m = Mounts()
    info = []
    for i in range(len(PC)):
        lower = f"/data/pcm/prep-{i}"
        r = be.call(op="attach_image", name=f"pc-{i:03d}", path=f"{F.BF}/pc-{i:03d}.img")
        m.mount("-t", "erofs", "-o", "ro", r["path"], lower)
        py = any(os.path.exists(lower + p) for p in ("/usr/bin/python3", "/usr/local/bin/python3"))
        rec = {"idx": i, "family": PC[i]["family"], "python3": py, "layers": len(PC[i]["manifest"]["layers"])}
        if py:
            t0 = time.time()
            r = sh("mkfs.erofs", "--quiet", f"--xattr-inode-digest={FP_NAME}", "--preserve-mtime",
                   f"{OUT}/pc-{i:03d}.erofs", lower, check=False)
            rec.update(mkfs_rc=r.returncode, mkfs_s=time.time() - t0, mkfs_err=r.stderr[-300:],
                       erofs_bytes=os.path.getsize(f"{OUT}/pc-{i:03d}.erofs") if r.returncode == 0 else 0)
            files = []
            for dp, dn, fn in os.walk(lower):
                for f in fn:
                    p = os.path.join(dp, f)
                    if os.path.isfile(p) and not os.path.islink(p):
                        files.append(p)
            random.Random(i).shuffle(files)
            rec["sample"] = [(p[len(lower):], hashlib.sha256(open(p, "rb").read()).hexdigest()) for p in files[:60]]
            rec["files"] = len(files)
        info.append(rec)
        print(json.dumps({k: rec.get(k) for k in ("idx", "family", "python3", "mkfs_rc", "mkfs_s", "erofs_bytes")}), flush=True)
    m.close()
    be.close()
    # Fingerprint format: what mkfs.erofs wrote, read back through a plain mount.
    fp = {}
    ok = [r for r in info if r.get("mkfs_rc") == 0]
    if ok:
        mnt = "/mnt/s13fp"
        os.makedirs(mnt, exist_ok=True)
        sh("mount", "-t", "erofs", "-o", "ro", f"{OUT}/pc-{ok[0]['idx']:03d}.erofs", mnt)
        for p, h in ok[0]["sample"][:5]:
            r = sh("getfattr", "--absolute-names", "-d", "-m", "-", "-e", "hex", mnt + p, check=False)
            fp[p] = {"content_sha256": h, "getfattr": r.stdout.strip().splitlines()[-3:]}
        sb = open(f"{OUT}/pc-{ok[0]['idx']:03d}.erofs", "rb").read(1024 + 128)[1024:]
        fp["superblock"] = {"feature_compat": hex(int.from_bytes(sb[8:12], "little")),
                            "feature_incompat": hex(int.from_bytes(sb[80:84], "little")),
                            "ishare_xattr_prefix_id(byte 105)": sb[105], "xattr_prefix_count(byte 91)": sb[91]}
        sh("umount", mnt, check=False)
    json.dump({"images": info, "fingerprint": fp}, open(f"{RES}/pc-prep.json", "w"), indent=1)
    print(json.dumps({"candidates": len(info), "with_python_and_erofs": len(ok), "fingerprint": fp}, indent=1), flush=True)


def run(a):
    prep_info = json.load(open(f"{RES}/pc-prep.json"))["images"]
    imgs = [r["idx"] for r in prep_info if r.get("python3") and r.get("mkfs_rc") == 0][: a.n]
    shutil.rmtree("/data/pcm", ignore_errors=True)
    v = a.variant
    dio = v.endswith("-dio")
    base_v = v[:-4] if dio else v
    be = None
    if base_v in ("image", "layer", "multidev"):
        F.BF = "/data/s13/pcbf"
        be = F.FanBackend(["--odirect"] if dio else [])
    m = Mounts()
    layer_mounts = {}
    time.sleep(1)
    drop_caches()
    time.sleep(2)
    m0 = meminfo()
    rss0 = rss(be.p.pid) if be else 0
    steps = []
    for k, i in enumerate(imgs):
        base = f"/data/pcm/{v}-{i}"
        t0 = time.time()
        new_layers = 0
        o = "ro,directio" if dio else "ro"
        if base_v == "image":
            r = be.call(op="attach_image", name=f"pc-{i:03d}", path=f"{F.BF}/pc-{i:03d}.img")
            m.mount("-t", "erofs", "-o", o, r["path"], f"{base}/lower")
            root = m.overlay(base, [f"{base}/lower"])
        elif base_v == "multidev":
            r = be.call(op="attach_multidev", name=f"pc-{i:03d}", dir="/data/s13/pcmd")
            m.mount("-t", "erofs", "-o", o + "," + ",".join(f"device={d}" for d in r["devices"]), r["boot"], f"{base}/lower")
            root = m.overlay(base, [f"{base}/lower"])
        elif base_v == "layer":
            lowers = []
            for l in PC[i]["manifest"]["layers"]:
                h = l["digest"].split(":")[1]
                if h not in layer_mounts:
                    if not os.path.exists(f"/data/s12/maps/layer-{h}.map.zst"):
                        continue  # an empty layer: no RAFS image
                    r = be.call(op="attach_image", name=f"layer-{h}", path=f"{F.BF}/layer-{h[:16]}.img")
                    mp = f"/data/pcm/layers/{h[:16]}"
                    m.mount("-t", "erofs", "-o", o, r["path"], mp)
                    layer_mounts[h] = mp
                    new_layers += 1
                lowers.append(layer_mounts[h])
            root = m.overlay(base, list(reversed(lowers)))
        else:
            opts = o + (",inode_share,domain_id=s13" if base_v == "ishare" else "")
            m.mount("-t", "erofs", "-o", opts, f"{OUT}/pc-{i:03d}.erofs", f"{base}/lower")
            root = m.overlay(base, [f"{base}/lower"])
        attach = time.time() - t0
        r = C.runsc(root, cfg(i), CMD, f"s13pc-{v}-{k}")
        time.sleep(0.2)
        mi = meminfo()
        step = {"k": k + 1, "idx": i, "family": PC[i]["family"], "attach_s": attach, "new_layer_mounts": new_layers,
                "cmd_s": r["wall"], "rc": r["rc"], "out": r["out"][-120:] if r["rc"] else "",
                "mem": {x: mi[x] - m0[x] for x in MEMKEYS}, "backend_rss_delta": (rss(be.p.pid) - rss0) if be else 0}
        steps.append(step)
        print(json.dumps({x: step[x] for x in ("k", "idx", "attach_s", "cmd_s", "rc", "new_layer_mounts")}),
              "cachedMB", round(step["mem"]["Cached"] / 2 ** 20, 1), "usedMB", round(-step["mem"]["MemAvailable"] / 2 ** 20, 1),
              flush=True)
    check = None
    if base_v in ("ishare", "erofs"):
        check = {"files": 0, "mismatch": 0, "missing": 0}
        for r in prep_info:
            if r["idx"] not in imgs:
                continue
            lower = f"/data/pcm/{v}-{r['idx']}/lower"
            for p, h in r["sample"]:
                check["files"] += 1
                try:
                    check["mismatch"] += hashlib.sha256(open(lower + p, "rb").read()).hexdigest() != h
                except OSError:
                    check["missing"] += 1
    st = be.call(op="stats") if be else None
    dmesg = sh("dmesg", check=False).stdout.splitlines()[-20:]
    m.close()
    if be:
        be.close()

    def slope(key, sign=1):
        xs = [s["k"] for s in steps[1:]]
        ys = [sign * s["mem"][key] for s in steps[1:]]
        if len(xs) < 2:
            return None
        mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
        return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    summary = {"variant": v, "n": len(steps), "failures": sum(1 for s in steps if s["rc"]),
               "first_image_cached_MB": steps[0]["mem"]["Cached"] / 2 ** 20,
               "first_image_used_MB": -steps[0]["mem"]["MemAvailable"] / 2 ** 20,
               "per_extra_image_cached_MB": (slope("Cached") or 0) / 2 ** 20,
               "per_extra_image_used_MB": (slope("MemAvailable", -1) or 0) / 2 ** 20,
               "per_extra_image_slab_MB": ((slope("SReclaimable") or 0) + (slope("SUnreclaim") or 0)) / 2 ** 20,
               "total_cached_MB": steps[-1]["mem"]["Cached"] / 2 ** 20,
               "total_used_MB": -steps[-1]["mem"]["MemAvailable"] / 2 ** 20,
               "attach_median_s": sorted(s["attach_s"] for s in steps)[len(steps) // 2],
               "cmd_median_s": sorted(s["cmd_s"] for s in steps)[len(steps) // 2],
               "layer_mounts": len(layer_mounts), "content_check": check,
               "fand_pack_bytes": st["counters"]["pack_bytes"] if st else None,
               "backend_rss_delta_MB": steps[-1]["backend_rss_delta"] / 2 ** 20}
    print(json.dumps(summary), flush=True)
    json.dump({"summary": summary, "steps": steps, "dmesg_tail": dmesg}, open(f"{RES}/pagecache-{v}.json", "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["prep", "run"])
    ap.add_argument("--variant", default="image")
    ap.add_argument("--n", type=int, default=64)
    a = ap.parse_args()
    prep(a) if a.what == "prep" else run(a)


if __name__ == "__main__":
    main()
